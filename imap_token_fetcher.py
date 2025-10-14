# imap_token_fetcher.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()


import os, re, time, imaplib, email, ssl, datetime as dt
from dataclasses import dataclass
from email.header import decode_header
from html import unescape
from typing import Optional, Tuple

TOKEN_PATTERNS = [
    re.compile(r"(?i)token(?:\s+de\s+verificaci[oó]n)?[:\s\-]*([A-Z0-9\-]{6,16})"),
    re.compile(r"(?i)c[oó]digo(?:\s+de\s+verificaci[oó]n)?[:\s\-]*([A-Z0-9\-]{6,16})"),
    re.compile(r"(?i)clave(?:\s+de\s+(?:verificaci[oó]n|acceso))?[:\s\-]*([A-Z0-9\-]{6,16})"),
]
GENERIC_CODE = re.compile(r"(?i)\b([A-Z0-9]{6,16})\b")

@dataclass
class ImapConfig:
    host: str
    port: int
    user: str
    password: str
    folder: str = "INBOX"
    sender_domain: str = "banxico.org.mx"
    subject_regex: str = r"(?i)(cep|token|banxico)"
    lookback_minutes: int = 60
    poll_interval: int = 5
    timeout_seconds: int = 600

    @staticmethod
    def from_env() -> "ImapConfig":
        return ImapConfig(
            host=os.getenv("IMAP_HOST", ""),
            port=int(os.getenv("IMAP_PORT", "993")),
            user=os.getenv("IMAP_USER", ""),
            password=os.getenv("IMAP_PASSWORD", ""),
            folder=os.getenv("IMAP_FOLDER", "INBOX"),
            sender_domain=os.getenv("IMAP_SENDER_DOMAIN", "banxico.org.mx"),
            subject_regex=os.getenv("IMAP_SUBJECT_REGEX", r"(?i)(cep|token|banxico)"),
            lookback_minutes=int(os.getenv("IMAP_LOOKBACK_MINUTES", "60")),
            poll_interval=int(os.getenv("IMAP_POLL_INTERVAL", "5")),
            timeout_seconds=int(os.getenv("IMAP_TIMEOUT_SECONDS", "600")),
        )

def _decode_header_value(h) -> str:
    if not h: return ""
    parts = decode_header(h)
    out = ""
    for val, enc in parts:
        if isinstance(val, bytes):
            try:
                out += val.decode(enc or "utf-8", errors="replace")
            except Exception:
                out += val.decode("utf-8", errors="replace")
        else:
            out += val
    return out

def _msg_to_text(msg) -> str:
    # Prefer text/plain; si no, text/html => strip tags simple
    texts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            if ctype in ("text/plain", "text/html"):
                try:
                    payload = part.get_payload(decode=True) or b""
                    charset = part.get_content_charset() or "utf-8"
                    txt = payload.decode(charset, errors="replace")
                    texts.append(txt)
                except Exception:
                    pass
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            texts.append(payload.decode(charset, errors="replace"))
        except Exception:
            pass
    text = "\n\n".join(texts)
    # Si es HTML, limpiar un poco
    if "<html" in text.lower():
        text = re.sub(r"(?is)<script.*?</script>", " ", text)
        text = re.sub(r"(?is)<style.*?</style>", " ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()

def _extract_token(body_text: str) -> Optional[str]:
    for pat in TOKEN_PATTERNS:
        m = pat.search(body_text)
        if m: return m.group(1).strip().upper()
    # fallback muy conservador: busca una palabra-código cerca de la palabra token
    win = re.compile(r"(?i)(token|c[oó]digo|clave)[^A-Z0-9]{0,30}([A-Z0-9]{6,16})")
    m = win.search(body_text)
    if m: return m.group(2).strip().upper()
    # último recurso (podría dar falsos positivos): cualquier “código” de 6–16 chars
    m = GENERIC_CODE.search(body_text)
    if m: return m.group(1).strip().upper()
    return None

def _imap_connect(c: ImapConfig):
    ctx = ssl.create_default_context()
    M = imaplib.IMAP4_SSL(c.host, c.port, ssl_context=ctx)
    M.login(c.user, c.password)
    M.select(c.folder, readonly=True)
    return M

def _since_for_search(minutes: int) -> str:
    # IMAP SINCE usa solo fecha (día), así que añadimos filtro propio por tiempo después
    since_date = (dt.datetime.utcnow() - dt.timedelta(minutes=minutes)).strftime("%d-%b-%Y")
    return since_date

def _search_ids(M, c: ImapConfig) -> list[str]:
    criteria = ['SINCE', _since_for_search(c.lookback_minutes)]
    # Filtro por FROM dominio si se puede
    try:
        status, data = M.search(None, 'SINCE', _since_for_search(c.lookback_minutes))
        if status != "OK": return []
        # Filtra en cliente por remitente/subject
        ids = data[0].split()
        out = []
        subj_re = re.compile(c.subject_regex)
        for i in ids[::-1]:  # más nuevos primero
            st, raw = M.fetch(i, '(BODY.PEEK[HEADER])')
            if st != "OK": continue
            hdr = raw[0][1] if raw and raw[0] else b""
            msg = email.message_from_bytes(hdr)
            from_h = _decode_header_value(msg.get("From"))
            subj_h = _decode_header_value(msg.get("Subject"))
            if c.sender_domain and c.sender_domain.lower() not in from_h.lower():
                continue
            if subj_re.search(subj_h or ""):
                out.append(i)
        return out
    except Exception:
        return []

def wait_for_token(cfg: ImapConfig, *, timeout_seconds: Optional[int]=None, poll_interval: Optional[int]=None) -> Optional[str]:
    deadline = time.time() + (timeout_seconds or cfg.timeout_seconds)
    pi = poll_interval or cfg.poll_interval
    last_seen_ids: set[str] = set()
    while time.time() < deadline:
        try:
            M = _imap_connect(cfg)
            try:
                ids = _search_ids(M, cfg)
                for i in ids:
                    if i in last_seen_ids: 
                        continue
                    st, raw = M.fetch(i, '(RFC822)')
                    if st != "OK": 
                        continue
                    msg = email.message_from_bytes(raw[0][1])
                    body = _msg_to_text(msg)
                    token = _extract_token(body)
                    if token:
                        return token
                    last_seen_ids.add(i)
            finally:
                try: M.logout()
                except Exception: pass
        except Exception:
            # pequeño backoff
            time.sleep(max(2, pi))
            continue
        time.sleep(pi)
    return None

# CLI rápido para probar conexión y extracción
if __name__ == "__main__":
    cfg = ImapConfig.from_env()
    print("Conectando IMAP a", cfg.host, "usuario", cfg.user, "carpeta", cfg.folder)
    tok = wait_for_token(cfg)
    if tok:
        print("TOKEN:", tok)
    else:
        print("No se encontró token dentro del timeout.")
