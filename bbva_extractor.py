# bbva_extractor.py
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber


# ===========================
# Utilidades generales
# ===========================

MES_ABREV = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}

AMOUNT_RE = re.compile(r"\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}")            # 3,536,667.93
DATE_TOKEN_SIMPLE = re.compile(r"(\d{2})[\/\-\s]([A-Za-zÁá]{3})")     # 03/ENE o 03 ENE

SKIP_PAGE_PAT = re.compile(
    r"(?i)cuadro\s+resumen\s+y\s+gr[aá]fico\s+de\s+movimientos\s+del\s+periodo"
)

# --- patrones para detectar aviso/pie de página de BBVA ---
NOTICE_PAT = re.compile(
    r"(?i)("
    r"estimad[oa]\s+cliente|"
    r"estado\s+de\s+cuenta\s+ha\s+sido\s+modificado|"
    r"ahora\s+tiene\s+m[aá]s\s+detalle|"
    r"contrato\s+ha\s+sido\s+modificado|"
    r"cualquier\s+sucursal|www\.bbva\.mx|"
    r"con\s+bbva\s+adelante"
    r")"
)

# ====== PATRONES DE METADATOS DE ENCABEZADO ======
RFC_FLEX_PAT   = re.compile(r"(?i)\bR\.?\s*F\.?\s*C\.?\s*[:\-]?\s*([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{2,3})\b")
NO_CUENTA_PAT  = re.compile(r"(?i)\bNo\.?\s*Cuenta[: ]+(\d+)\b")
NO_CLIENTE_PAT = re.compile(r"(?i)\bNo\.?\s*Cliente[: ]+([A-Z0-9]+)\b")
CLABE_PAT      = re.compile(r"(?i)\bCLABE[: ]+(\d{18})\b")

# Empresa (razón social) típica: S.A. de C.V., S. de R.L., etc.
EMPRESA_PAT = re.compile(
    r"(?i)\b([A-ZÁÉÍÓÚÑ0-9&\.\-,' ]{6,}?("
    r"S\.?\s*A\.?(?:\s*DE)?\s*C\.?\s*V\.?|"
    r"S\.?\s*DE\s*R\.?\s*L\.?|"
    r"S\.?\s*DE\s*C\.?\s*V\.?"
    r"))\b"
)

FOOTER_PAT = re.compile(
    r"(?ix)("
    r"bbva\s+m[eé]xico[,.\s]+s\.?a\.?,?\s*instituci[oó]n\s+de\s+banca\s+m[uú]ltiple.*?grupo\s+financiero\s+bbva(?:\s+m[eé]xico)?"
    r"|av\.?\s*paseo\s+de\s+la\s+reforma\s*510.*?(?:c\.?\s*p\.?\s*\d{5}|ciudad\s+de\s+m[eé]xico|m[eé]xico)"
    r"|r\.?\s*f\.?\s*c\.?\s*bba[a-z0-9]{6,}"
    r")"
)

# Eliminadores explícitos (dirección / rótulos de corte intermedios)
FOOTER_EXTRA_PAT = re.compile(
    r"(?i)reforma\s*510.*?ju[aá]rez.*?cuauh?t[eé]moc.*?c\.?\s*p\.?\s*0?6600.*?ciudad",
    re.DOTALL,
)

# “INSTITUCIÓN DE BANCA MÚLTIPLE, GRUPO FINANCIERO” (a veces precedido de “S.A.,”)
ORG_MID_PHRASE_PAT = re.compile(
    r"(?i)\b(?:S\.?\s*A\.?,?\s*)?INSTITUCI[ÓO]N\s+DE\s+BANCA\s+M[ÚU]LTIPLE,?\s+GRUPO\s+FINANCIERO\b"
)

# Fila “línea punteada”: muchas rayas/guiones/guiones largos seguidos
DASH_ROW_PAT = re.compile(r"(?:[-–—_]{2,}\s*){6,}")

# Ruido al cruzar de página: “S.A.”
CROSS_SA_PAT = re.compile(r"(?i)\bS\.?\s*A\.?[,\.]?\b")

HEADER_TOKENS = {
    "OPER", "LIQ", "CARGOS", "ABONOS",
    "OPERACIÓN", "OPERACION", "LIQUIDACIÓN", "LIQUIDACION",
    "SALDO", "FECHA"
}


def _norm_amount(s: Optional[str]) -> float:
    if not s:
        return 0.0
    txt = (
        s.replace("\u00a0", " ")
         .replace("$", "")
         .replace(" ", "")
         .replace(",", "")
    )
    try:
        return float(txt)
    except Exception:
        return 0.0


def _guess_year_from_period(full_text: str) -> Optional[int]:
    pats = [
        r"Periodo\s+del\s+\d{2}[\/\-\s][A-Za-zÁá]{3}[\/\-\s](\d{4})\s+al\s+\d{2}[\/\-\s][A-Za-zÁá]{3}[\/\-\s](\d{4})",
        r"Del\s+\d{2}[\/\-\s][A-Za-zÁá]{3}\s+al\s+\d{2}[\/\-\s][A-Za-zÁá]{3}\s+de\s+(\d{4})",
        r"(\d{2})[\/\-](\d{2})[\/\-](\d{4})\s+a\s+(\d{2})[\/\-](\d{2})[\/\-](\d{4})",
    ]
    for pat in pats:
        m = re.search(pat, full_text, flags=re.IGNORECASE)
        if m:
            try:
                y = int(m.groups()[-1])
                return y
            except Exception:
                pass
    return None


def _parse_period(full_text: str) -> Tuple[Optional[date], Optional[date]]:
    pats = [
        r"Periodo\s+del\s+(\d{2})[\/\-\s]([A-Za-zÁá]{3})[\/\-\s](\d{4})\s+al\s+(\d{2})[\/\-\s]([A-Za-zÁá]{3})[\/\-\s](\d{4})",
        r"Del\s+(\d{2})[\/\-\s]([A-Za-zÁá]{3})\s+al\s+(\d{2})[\/\-\s]([A-Za-zÁá]{3})\s+de\s+(\d{4})",
    ]
    for pat in pats:
        m = re.search(pat, full_text, flags=re.IGNORECASE)
        if not m:
            continue
        g = [x for x in m.groups() if x is not None]
        try:
            if len(g) == 6:  # dd mon yyyy dd mon yyyy
                d1, mon1, y1, d2, mon2, y2 = g
                y1, y2 = int(y1), int(y2)
            else:            # dd mon dd mon yyyy
                d1, mon1, d2, mon2, y = g
                y1 = y2 = int(y)
            m1 = MES_ABREV[mon1.lower().replace("á", "a")]
            m2 = MES_ABREV[mon2.lower().replace("á", "a")]
            return date(y1, m1, int(d1)), date(y2, m2, int(d2))
        except Exception:
            pass
    return None, None


def _parse_ddmon(token: str, fallback_year: Optional[int]) -> Optional[date]:
    """Convierte '03/ENE' o '03-ENE' a date con el año indicado."""
    m = DATE_TOKEN_SIMPLE.search(token.replace("\u00a0", " "))
    if not m or not fallback_year:
        return None
    d = int(m.group(1))
    mon = MES_ABREV.get(m.group(2).lower().replace("á", "a"))
    if not mon:
        return None
    return date(fallback_year, mon, d)


@dataclass
class TxRow:
    f_oper: Optional[date] = None
    f_liq: Optional[date] = None
    desc: str = ""
    cargos: float = 0.0
    abonos: float = 0.0
    s_oper: Optional[float] = None
    s_liq: Optional[float] = None


# ===========================
# Núcleo de parsing por coordenadas
# ===========================

def _detect_columns(words: List[dict], page_width: float) -> Tuple[List[Tuple[float, float]], float]:
    """
    Devuelve bounds [(x0,x1) ...] para columnas:
    [OPER, LIQ, DESCRIP, REFER, CARGOS, ABONOS, SOPER, SLIQ]
    y el 'header_y' (bottom del encabezado) para descartar títulos.
    """
    hits: Dict[str, List[dict]] = {}
    labels = {
        "CARGOS": ["CARGOS"],
        "ABONOS": ["ABONOS"],
        "OPERACION": ["OPERACIÓN", "OPERACION"],
        "LIQUIDACION": ["LIQUIDACIÓN", "LIQUIDACION"],
        "CODDESC": ["COD.", "DESCRIPCIÓN", "DESCRIPCION"],
        "REFERENCIA": ["REFERENCIA"],
        "OPER": ["OPER"],
        "LIQ": ["LIQ"],
    }
    for w in words:
        t = w["text"].upper()
        for key, opts in labels.items():
            if any(o in t for o in opts):
                hits.setdefault(key, []).append(w)

    def cx(key, default_frac):
        if key in hits:
            w = min(hits[key], key=lambda x: x["top"])
            return (w["x0"] + w["x1"]) / 2
        return page_width * default_frac

    x_oper = cx("OPER", 0.08)
    x_liq = cx("LIQ", 0.13)
    x_desc = cx("CODDESC", 0.33)
    x_ref = cx("REFERENCIA", 0.51)
    x_car = cx("CARGOS", 0.70)
    x_abo = cx("ABONOS", 0.78)
    x_sop = cx("OPERACION", 0.86)
    x_sliq = cx("LIQUIDACION", 0.94)

    xs = sorted([x_oper, x_liq, x_desc, x_ref, x_car, x_abo, x_sop, x_sliq])

    def mid(a, b):
        return (a + b) / 2

    bounds = [
        (0,                  mid(xs[0], xs[1]) - 1),   # OPER
        (mid(xs[0], xs[1]) + 1, mid(xs[1], xs[2]) - 1),  # LIQ
        (mid(xs[1], xs[2]) + 1, mid(xs[2], xs[3]) - 1),  # DESCRIP
        (mid(xs[2], xs[3]) + 1, mid(xs[3], xs[4]) - 1),  # REFER
        (mid(xs[3], xs[4]) + 1, mid(xs[4], xs[5]) - 1),  # CARGOS
        (mid(xs[4], xs[5]) + 1, mid(xs[5], xs[6]) - 1),  # ABONOS
        (mid(xs[5], xs[6]) + 1, mid(xs[6], xs[7]) - 1),  # S OPER
        (mid(xs[6], xs[7]) + 1, page_width),             # S LIQ
    ]

    header_y = 0.0
    if hits:
        top_candidates = []
        for k in hits:
            w = min(hits[k], key=lambda x: x["top"])
            top_candidates.append(w["bottom"])
        if top_candidates:
            header_y = max(top_candidates)
    return bounds, header_y


def _group_rows(words: List[dict], header_y: float) -> List[List[dict]]:
    """Agrupa palabras en filas usando binning por Y."""
    # Relajamos el corte debajo del encabezado para no perder la primera línea
    body = [w for w in words if w["top"] > header_y + 1.0]
    if not body:
        return []
    heights = [(w["bottom"] - w["top"]) for w in body]
    avg_h = (sum(heights) / len(heights)) if heights else 8.0
    ybin = max(2.0, min(4.2, avg_h * 0.70))

    rows: Dict[int, List[dict]] = {}
    for w in body:
        yk = int(round(w["top"] / ybin))
        rows.setdefault(yk, []).append(w)

    return [sorted(rows[k], key=lambda x: x["x0"]) for k in sorted(rows.keys())]


def _assign_cols(row_words: List[dict], bounds: List[Tuple[float, float]]) -> List[str]:
    cols = [""] * 8
    for w in row_words:
        xc = (w["x0"] + w["x1"]) / 2
        text = w["text"]
        for i, (x0, x1) in enumerate(bounds):
            if x0 <= xc <= x1:
                cols[i] = (cols[i] + " " + text).strip() if cols[i] else text
                break
    return [c.strip() for c in cols]


# --- helper: recorta pies/avisos incrustados en la descripción ---
def _strip_bbva_footer(text: str) -> str:
    """
    Elimina, aunque vengan pegados dentro de la descripción, los bloques
    típicos del pie/aviso de BBVA (razón social, dirección, RFC, aviso).
    """
    if not text:
        return text
    t = str(text)
    for pat in (ORG_MID_PHRASE_PAT, FOOTER_EXTRA_PAT, FOOTER_PAT, NOTICE_PAT):
        t = re.sub(pat, " ", t)
    # Limpieza de espacios y signos colgantes
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[ ,;:\-–—]+$", "", t).strip()
    return t


def _strip_crosspage_noise(text: str, is_second: bool) -> str:
    """Ruido que aparece cuando un movimiento continúa en la página siguiente."""
    if not text:
        return text
    t = CROSS_SA_PAT.sub(" ", str(text))
    if is_second:
        # refuerza limpieza de posibles restos del bloque de aviso
        for pat in (
            re.compile(r"(?i)\bcliente\b"),
            re.compile(r"(?i)\bahora\b"),
            re.compile(r"(?i)\bestimad[oa]\b"),
            re.compile(r"(?i)\bbbva\s+m[eé]xico(?:\s+de\s+m[eé]xico)?\b"),
            re.compile(r"(?i)\bcontrato\b"),
            re.compile(r"(?i)\bcualquier\s+adelante\b"),
        ):
            t = pat.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[ ,;:\-–—]+$", "", t).strip()
    return t


def _maybe_fix_amount_columns(tx: TxRow) -> None:
    """
    Heurística: si Cargos/Abonos están en 0 pero hay un valor en Saldos,
    re-clasificar ese valor como Cargo/Abono según la descripción.
    """
    cargos_zero = (tx.cargos or 0) == 0
    abonos_zero = (tx.abonos or 0) == 0
    saldos = [v for v in (tx.s_oper, tx.s_liq) if v is not None and v != 0]

    if cargos_zero and abonos_zero and len(saldos) == 1:
        v = saldos[0]
        u = (tx.desc or "").upper()

        debit_kw = (
            "ENVIADO", "PAGO", "COMPRA", "RETIRO", "DÉBITO", "DEBITO",
            "COMISION", "COMISIÓN", "DOMICILI", "N06", "CARGO", "SPEI"
        )
        credit_kw = (
            "ABONO", "DEPÓSITO", "DEPOSITO", "NÓMINA", "NOMINA",
            "RECIBIDO", "INTERESES A FAVOR", "DEVOLUCIÓN", "DEVOLUCION"
        )

        is_credit = any(k in u for k in credit_kw)
        is_debit  = any(k in u for k in debit_kw)

        if is_credit and not is_debit:
            tx.abonos = v
        else:
            tx.cargos = v

        tx.s_oper = None
        tx.s_liq = None


def _clean_movements_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) Recorta de cada descripción los fragmentos de pie/aviso BBVA.
    2) Elimina filas de boilerplate (avisos, totales, etc.).
    3) Concatena a la fila anterior las líneas sin fechas ni montos que sólo
       continúan la descripción (incluye BNET/dígitos).
    4) Elimina filas totalmente vacías.
    """
    if df.empty:
        return df

    df = df.copy()
    df["Descripción"] = df["Descripción"].fillna("").astype(str).apply(_strip_bbva_footer)

    boiler_pat = re.compile(
        r"(?ix)("
        r"estimad[oa]\s+cliente|ahora\s+tiene\s+m[aá]s\s+detalle|"
        r"contrato\s+ha\s+sido\s+modificado|www\.bbva\.mx|con\s+bbva\s+adelante|"
        r"total\s+de\s+movimientos|total\s+importe\s+cargos?|total\s+importe\s+abonos?|"
        r"total\s+movimientos\s+cargos?|total\s+movimientos\s+abonos?|"
        r"cuadro\s+resumen\s+y\s+gr[aá]fico\s+de\s+movimientos\s+del\s+periodo|"
        r"la\s+gat\s+real|nota:\s+en\s+la\s+columna\s+porcentaje|"
        r"los\s+montos\s+m[ií]nimos\s+requeridos|para\s+mayor\s+informaci[oó]n\s+consulta|"
        r"bbva\s+m[eé]xico[,.\s]+s\.?a\.?,?\s*instituci[oó]n\s+de\s+banca\s+m[uú]ltiple|"
        r"av\.?\s*paseo\s+de\s+la\s+reforma\s*510|r\.?\s*f\.?\s*c\.?\s*bba[a-z0-9]{6,}"
        r")"
    )
    df = df[~df["Descripción"].str.contains(boiler_pat, regex=True)].reset_index(drop=True)

    numeric = ["Cargos", "Abonos", "Saldo Operación", "Saldo Liquidación"]
    num = df[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    def _empty_date(v) -> bool:
        return pd.isna(v) or (isinstance(v, str) and not v.strip())

    rows: List[dict] = []
    for i, r in df.iterrows():
        no_dates = _empty_date(r["Fecha Operación"]) and _empty_date(r["Fecha Liquidación"])
        zero_money = bool((num.iloc[i] == 0).all())
        text = str(r["Descripción"]).strip()
        has_text = bool(text)

        # (3) Continuación de descripción (sin fechas/montos y con texto)
        if rows and no_dates and zero_money and has_text:
            rows[-1]["Descripción"] = (str(rows[-1]["Descripción"]).strip() + " " + text).strip()
            continue

        # (4) Fila totalmente vacía
        if no_dates and not has_text and zero_money:
            continue

        rows.append(r.to_dict())

    out = pd.DataFrame(rows, columns=df.columns)
    out["Descripción"] = out["Descripción"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    return out.reset_index(drop=True)


# ====== META-EXTRACCIÓN DE ENCABEZADO ======
def _extract_header_metadata(full_text: str, pdf_path: str) -> Dict[str, Optional[str]]:
    """
    Devuelve empresa, rfc, cuenta, cliente, producto y clabe (si aparecen).
    Evita capturar el RFC del banco (BBVA) cuando sólo aparece el del pie legal.
    """
    meta = {"empresa": None, "rfc": None, "cuenta": None, "cliente": None, "producto": None, "clabe": None}
    txt = full_text.replace("\u00a0", " ")
    lines = [ln.strip() for ln in txt.splitlines()]

    # No. de cuenta / cliente / clabe
    m = NO_CUENTA_PAT.search(txt)
    if m: meta["cuenta"] = m.group(1)
    m = NO_CLIENTE_PAT.search(txt)
    if m: meta["cliente"] = m.group(1)
    m = CLABE_PAT.search(txt)
    if m: meta["clabe"] = m.group(1)

    # Producto: intenta tomar la línea siguiente a “Estado de Cuenta”
    for i, ln in enumerate(lines):
        if re.search(r"(?i)estado\s+de\s+cuenta", ln):
            for j in range(1, 4):
                if i + j < len(lines):
                    cand = lines[i + j].strip()
                    if cand and not re.search(r"(?i)P[ÁA]GINA|No\.?\s*Cuenta|No\.?\s*Cliente", cand):
                        meta["producto"] = cand
                        break
            break

    # RFC (flex) – descarta RFC del banco (BBA… / BBM…); si sólo hay esos, deja None
    rfc_hits = [r.upper() for r in RFC_FLEX_PAT.findall(txt)]
    meta["rfc"] = next((r for r in rfc_hits if not r.startswith(("BBA", "BBM"))), None)

    # Empresa: razón social típica en primeras líneas, ignorando las del banco
    for ln in lines[:120]:
        up = ln.upper()
        if "BBVA MEXICO" in up or "INSTITUCI" in up:
            continue
        em = EMPRESA_PAT.search(ln)
        if em:
            meta["empresa"] = em.group(1).strip()
            break

    # Fallback de empresa por nombre del archivo (prefijo antes de _0xxxx…)
    if not meta["empresa"]:
        stem = Path(pdf_path).stem
        m = re.match(r"([A-Za-zÁÉÍÓÚÑ&\.\- ']+?)_0?\d", stem)
        if m:
            meta["empresa"] = m.group(1).replace("_", " ").strip()

    return meta


# ===========================
# Extractor principal
# ===========================

def extract_bbva_to_xlsx(pdf_path: str, out_xlsx: str) -> None:
    # 1) Leemos todo el texto para periodo + encabezados
    with pdfplumber.open(pdf_path) as pdf:
        all_text = "\n".join((p.extract_text() or "") for p in pdf.pages).replace("\u00a0", " ")

    pstart, pend = _parse_period(all_text)
    year_hint = (pstart or pend or date.today()).year
    if not pstart or not pend:
        y = _guess_year_from_period(all_text)
        if y:
            year_hint = y

    # ===== Metadatos del encabezado / caja de cuenta =====
    meta = _extract_header_metadata(all_text, pdf_path)
    empresa  = meta["empresa"]
    rfc      = meta["rfc"]
    cuenta   = meta["cuenta"]
    ncliente = meta["cliente"]
    producto = meta["producto"]
    clabe    = meta["clabe"]

    # 2) Parseo de movimientos por coordenadas
    txs: List[TxRow] = []
    current: Optional[TxRow] = None

    with pdfplumber.open(pdf_path) as pdf:
        second_cross_active = False   # estamos continuando el 2º movimiento en nueva página
        current_tx_index: Optional[int] = None  # índice (0-based) del movimiento en curso
        hard_stop_after_this_page = False       # al ver "Total de Movimientos" cortamos tras la página

        for page_idx, page in enumerate(pdf.pages, start=1):
            page_text = (page.extract_text() or "").replace("\u00a0", " ")

            # Saltar la página de "Cuadro resumen..."
            if SKIP_PAGE_PAT.search(page_text):
                continue

            words = page.extract_words(x_tolerance=2, y_tolerance=2, use_text_flow=True)
            bounds, header_y = _detect_columns(words, page.width)
            rows = _group_rows(words, header_y)

            page_stop = False
            saw_totals_here = False

            for r in rows:
                row_full_txt = " ".join(w["text"] for w in r)
                row_full_up = row_full_txt.upper()

                # 1) Línea punteada o “Estimado Cliente” -> se ignora todo lo que esté abajo
                if DASH_ROW_PAT.search(row_full_txt) or ("ESTIMADO" in row_full_up and "CLIENTE" in row_full_up):
                    page_stop = True
                    break

                # 2) “Total de Movimientos” -> procesar lo de arriba y cortar al finalizar la página
                if "TOTAL DE MOVIMIENTOS" in row_full_up:
                    page_stop = True
                    saw_totals_here = True
                    break

                cols = _assign_cols(r, bounds)
                oper_raw, liq_raw, descr_raw, ref_raw, car_raw, abo_raw, sop_raw, sliq_raw = cols

                # ----- Fallback para recuperar descripción si la detección de columnas falla -----
                fallback_desc_tokens: List[str] = []
                for w in r:
                    txt = w["text"]
                    if AMOUNT_RE.search(txt):
                        continue
                    if DATE_TOKEN_SIMPLE.search(txt):
                        continue
                    if txt.upper() in HEADER_TOKENS:
                        continue
                    fallback_desc_tokens.append(txt)
                fallback_desc = " ".join(fallback_desc_tokens).strip()

                desc_piece = " ".join(x for x in [descr_raw, ref_raw] if x).strip()
                if not desc_piece:
                    desc_piece = fallback_desc
                desc_piece = _strip_bbva_footer(desc_piece)

                # Si estamos en la continuación del 2º movimiento, elimina tokens y “S.A.”
                if second_cross_active and current is not None and current_tx_index == 1:
                    desc_piece = _strip_crosspage_noise(desc_piece, is_second=True)
                else:
                    desc_piece = _strip_crosspage_noise(desc_piece, is_second=False)

                # Filtros de renglones basura / separadores
                header_line = ("DETALLE DE MOVIMIENTOS" in (descr_raw.upper() + ref_raw.upper()))
                if header_line:
                    continue
                dashed = (descr_raw.strip().startswith("—") or descr_raw.strip().startswith("- -"))
                if dashed:
                    continue

                no_dates = not oper_raw and not liq_raw
                no_amounts = not (AMOUNT_RE.search(car_raw) or AMOUNT_RE.search(abo_raw) or
                                  AMOUNT_RE.search(sop_raw) or AMOUNT_RE.search(sliq_raw))

                # descartar aviso/pie como fila suelta
                if no_dates and no_amounts and desc_piece and (NOTICE_PAT.search(desc_piece) or FOOTER_PAT.search(desc_piece)):
                    continue

                # 3) Si no hay fechas ni importes -> continuación de descripción
                if no_dates and no_amounts and desc_piece:
                    if current:
                        current.desc = (current.desc + " " + desc_piece).strip()
                    continue

                # 4) Si llega una nueva fila con fecha -> cerramos la anterior
                if oper_raw or liq_raw:
                    if current:
                        txs.append(current)
                        current = None
                        second_cross_active = False  # empieza un movimiento nuevo

                    f_oper = _parse_ddmon(oper_raw, year_hint) if oper_raw else None
                    f_liq = _parse_ddmon(liq_raw, year_hint) if liq_raw else None
                    current_tx_index = len(txs)  # índice del movimiento que comienza
                    current = TxRow(
                        f_oper=f_oper,
                        f_liq=f_liq,
                        desc=desc_piece,
                    )
                else:
                    # No hay fecha pero sí importes -> agrega a la actual
                    if current and desc_piece:
                        current.desc = (current.desc + " " + desc_piece).strip()

                # 5) Montos si existieran en esta línea
                if current:
                    if car_raw:
                        current.cargos = _norm_amount(car_raw)
                    if abo_raw:
                        current.abonos = _norm_amount(abo_raw)
                    if sop_raw:
                        val = _norm_amount(sop_raw)
                        current.s_oper = val if sop_raw.strip() else current.s_oper
                    if sliq_raw:
                        val = _norm_amount(sliq_raw)
                        current.s_liq = val if sliq_raw.strip() else current.s_liq

                    # Arreglo heurístico si los importes cayeron en saldos
                    _maybe_fix_amount_columns(current)

            # Al terminar la página:
            if current is not None and current_tx_index == 1:
                # Si hay un movimiento abierto y es el 2º, activar modo "cruce" para la siguiente página
                second_cross_active = True
            else:
                second_cross_active = False

            if saw_totals_here:
                hard_stop_after_this_page = True

            if page_stop:
                # ignoramos el resto de la página; pasamos a la siguiente
                pass

            if hard_stop_after_this_page:
                break

        # al acabar páginas, volcar el pendiente
        if current:
            txs.append(current)

    # Red de seguridad: limpiar tokens en TODO el texto del 2º movimiento (si existe)
    if len(txs) >= 2 and txs[1].desc:
        txs[1].desc = _strip_crosspage_noise(txs[1].desc, is_second=True)

    # Fechas como texto
    def fmt(d: Optional[date]) -> Optional[str]:
        return d.strftime("%d-%m-%Y") if isinstance(d, date) else None

    movs_df = pd.DataFrame([{
        "Fecha Operación": fmt(t.f_oper),
        "Fecha Liquidación": fmt(t.f_liq),
        "Descripción": re.sub(r"\s+", " ", (t.desc or "")).strip(),
        "Cargos": round(t.cargos or 0.0, 2),
        "Abonos": round(t.abonos or 0.0, 2),
        "Saldo Operación": round(t.s_oper, 2) if t.s_oper is not None else None,
        "Saldo Liquidación": round(t.s_liq, 2) if t.s_liq is not None else None,
    } for t in txs], columns=[
        "Fecha Operación", "Fecha Liquidación", "Descripción",
        "Cargos", "Abonos", "Saldo Operación", "Saldo Liquidación"
    ])

    # ===== Fallback de periodo usando las fechas de movimientos =====
    if not pstart or not pend:
        fechas = []
        for t in txs:
            if isinstance(t.f_oper, date): fechas.append(t.f_oper)
            if isinstance(t.f_liq,  date): fechas.append(t.f_liq)
        if fechas:
            pstart = min(fechas)
            pend   = max(fechas)

    # 3) Hojas info y cuenta
    info_df = pd.DataFrame([{
        "Banco": "BBVA",
        "Archivo": Path(pdf_path).name,
        "Periodo inicio": pstart.strftime("%d-%m-%Y") if pstart else None,
        "Periodo fin":    pend.strftime("%d-%m-%Y") if pend else None,
        "Empresa": empresa,
        "RFC": rfc
    }])

    cuenta_df = pd.DataFrame([{
        "No. de cuenta": cuenta,
        "No. Cliente": ncliente,
        "CLABE": clabe,
        "Producto": producto,
        "Moneda": "MXN"
    }])

    # Limpieza final de movimientos
    movs_df = _clean_movements_df(movs_df)

    # 4) Escribir Excel
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        info_df.to_excel(xl, index=False, sheet_name="info")
        cuenta_df.to_excel(xl, index=False, sheet_name="cuenta")
        movs_df.to_excel(xl, index=False, sheet_name="movimientos")
