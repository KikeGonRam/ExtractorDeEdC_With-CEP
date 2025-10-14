# santander_extractor.py
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

TOTAL_PAT = re.compile(r"(?i)\bTOTAL\b")
SALDO_FINAL_PAT = re.compile(r"(?i)SALDO\s+FINAL\s+DEL\s+PERIODO")

def _has_total_or_final(s: str) -> bool:
    return bool(TOTAL_PAT.search(s) or SALDO_FINAL_PAT.search(s))


# Montos como 3,536,667.93 (se usa en columnas y para recortar el "tail")
AMOUNT_RE = re.compile(r"\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}")
DATE_TOKEN_SANT = re.compile(r"(\d{2})\s*-\s*([A-Za-zÁá]{3})\s*-\s*(\d{4})")


# Excepción BANCO INVEX: tokens tipo 0,000,070.69 deben ser parte de la descripción
BANCOINVEX_TAG_RE = re.compile(r"(?i)BANCO\s*INVEX")
BANCOINVEX_FAKE_AMT_RE = re.compile(r"\b0(?:,\d{3}){2,}\.\d{2}\b")

# Encabezados de sección
HEAD_CUENTA = re.compile(r"(?im)^\s*Detalle(?:s)?\s+de\s+movimientos\s+cuenta\s+de\s+cheques\b")
HEAD_INVER  = re.compile(r"(?im)^\s*Detalle(?:s)?\s+de\s+movimientos\s+Dinero\s+Creciente\s+Santander\b")

# Producto
PROD_CUENTA = re.compile(r"(?im)^\s*CUENTA\s+SANTANDER\s+PYME\s+(\d{2}-\d{8}-\d)\b")
PROD_INVER  = re.compile(r"(?im)^\s*INVERSION\s+CRECIENTE\s+(\d{2}-\d{8}-\d)\b")

SALDO_ANT_PAT = re.compile(r"(?i)SALDO\s+FINAL\s+DEL\s+PERIODO\s+ANTERIOR\s*:\s*\$?\s*([\d,]+\.\d{2})")

# Metadatos
EMPRESA_PAT = re.compile(r"(?m)^[A-ZÁÉÍÓÚÑ&\.\-,' ]{6,}$")
RFC_PAT     = re.compile(r"(?i)\bR\.?\s*F\.?\s*C\.?\s*[: ]+([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{2,3})\b")
CLIENTE_PAT = re.compile(r"(?i)C[ÓO]DIGO\s+DE\s+CLIENTE\s+NO\.?\s*([A-Z0-9]+)")
CLABE_PAT   = re.compile(r"(?i)CUENTA\s+CLABE[: ]+(\d{18})")
PERIODO_PAT = re.compile(
    r"(?i)PERIODO\s+DEL\s+(\d{2}\s*-\s*[A-Za-zÁá]{3}\s*-\s*\d{4})\s+AL\s+(\d{2}\s*-\s*[A-Za-zÁá]{3}\s*-\s*\d{4})"
)
CLAVE_RASTREO_PAT = re.compile(r"(?i)\bCLAVE\s+DE\s+RASTREO\b")


# Ruido / tokens
HEADER_TOKENS = {"FECHA", "FOLIO", "DESCRIPCION", "DESCRIPCIÓN", "DEPÓSITO", "DEPOSITO", "RETIRO", "SALDO"}

# Información fiscal y basura de folios/paginación
INFO_FISCAL_PAT = re.compile(
    r"(?i)\b(UUID|CFDI|TIMBRAD[OA]|COMPROBANTE|SELLO\s+DIGITAL|CADENA\s+ORIGINAL|SAT|"
    r"REG[IÍ]MEN|EMISOR|RECEPTOR|USO\s+CFDI|FOLIO\s+INTERNO|FORMA\s+DE\s+PAGO|M[EÉ]TODO\s+DE\s+PAGO|"
    r"FECHA\s+Y\s+HORA\s+DE|CERTIFICACI[ÓO]N|CSD|COMPLEMENTO)\b"
)
BASE64ISH_PAT   = re.compile(r"[A-Za-z0-9+/=]{20,}")
PAGINA_WORD_PAT = re.compile(r"(?i)\bP[\W_]*GINA\s*\d+\s*DE\s*\d+\b")
PP_NUM_PAT      = re.compile(r"(?i)\bP-?P\.?\s*\d+\b")
DETALLES_HEADER_PAT = re.compile(r"(?i)Detalle[s]?\s+de\s+movimientos")


# ===========================
# Helpers
# ===========================

def _drop_footer_totals(words: List[dict]) -> List[dict]:
    """
    Quita de la página todo lo que esté en la fila de 'TOTAL' y por debajo
    (incluye 'SALDO FINAL DEL PERIODO'), para que no se mezclen con el último
    movimiento.
    """
    y_cut = None
    for w in words:
        t = (w.get("text") or "").replace("\u00a0", " ").strip().upper()
        if t == "TOTAL" or "SALDO FINAL DEL PERIODO" in t:
            y_cut = w["top"] if y_cut is None else min(y_cut, w["top"])
    if y_cut is None:
        return words
    # pequeño margen para no tocar el renglón superior
    return [w for w in words if w["top"] < y_cut - 0.5]

def _cut_after_totals(s: str) -> str:
    """Si la línea contiene 'TOTAL' o 'SALDO FINAL DEL PERIODO', corta desde ahí a la derecha."""
    m1 = TOTAL_PAT.search(s)
    m2 = SALDO_FINAL_PAT.search(s)
    m = None
    if m1 and m2:
        m = m1 if m1.start() < m2.start() else m2
    else:
        m = m1 or m2
    return s[:m.start()].rstrip() if m else s


def _is_garbage_piece(s: str) -> bool:
    """
    True si el fragmento es basura (paginación/CFDI/base64ish), salvo que contenga 'CLAVE DE RASTREO'.
    """
    if not s:
        return True
    if CLAVE_RASTREO_PAT.search(s):
        return False
    if INFO_FISCAL_PAT.search(s):
        return True
    if BASE64ISH_PAT.search(s):
        return True
    return False


def _strip_page_garbage(s: str) -> str:
    """Quita paginación y basura “P-P 12345”, y compacta espacios."""
    s = PAGINA_WORD_PAT.sub("", s)
    s = PP_NUM_PAT.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_amount(s: Optional[str]) -> float:
    if not s:
        return 0.0
    txt = s.replace("\u00a0", " ").replace("$", "").replace(" ", "").replace(",", "")
    try:
        return float(txt)
    except Exception:
        return 0.0

def _pick_amount(s: Optional[str], prefer: str = "first") -> Optional[float]:
    if not s:
        return None
    t = s.replace("\u00a0", " ").strip()
    if t in {"0", "0.0", "0.00"}:
        return 0.0
    m = AMOUNT_RE.findall(t)
    if not m:
        return None
    val = m[0] if prefer == "first" else m[-1]
    return _norm_amount(val)

def _parse_ddmonyyyy(token: str) -> Optional[date]:
    m = DATE_TOKEN_SANT.search(token.replace("\u00a0", " ").upper())
    if not m:
        return None
    dd = int(m.group(1))
    mon = MES_ABREV.get(m.group(2).lower().replace("á", "a"))
    yy = int(m.group(3))
    if not mon:
        return None
    return date(yy, mon, dd)

def _extract_tail_run(line: str, maxn: int = 3) -> Tuple[str, List[float]]:
    """
    De 'line' regresa (desc_sin_importes_finales, [importes...]),
    tomando el bloque contiguo más a la derecha con 2–3 importes con decimales.
    Excepción: si la línea contiene BANCO(INVEX), números tipo 0,000,xxx.xx
    se tratan como texto (no importes).
    """
    s = _strip_page_garbage(line.replace("\u00a0", " ").rstrip())

    # Buscar importes
    ms = list(AMOUNT_RE.finditer(s))

    # --- Excepción BANCO INVEX: descartar "importes" como 0,000,070.69 / 0,000,000.02
    if BANCOINVEX_TAG_RE.search(s):
        ms = [
            m for m in ms
            if not BANCOINVEX_FAKE_AMT_RE.fullmatch(
                m.group(0).replace("$", "").replace(" ", "")
            )
        ]

    if not ms:
        return s, []

    # Tomar el bloque contiguo más a la derecha (dep, retiro, saldo)
    tail = [ms[-1]]
    cur_start = ms[-1].start()
    j = len(ms) - 2
    while j >= 0 and len(tail) < maxn:
        between = s[ms[j].end():cur_start]
        if between.strip():
            break
        tail.append(ms[j])
        cur_start = ms[j].start()
        j -= 1
    tail.reverse()

    desc = s[:cur_start].rstrip()
    vals = [_norm_amount(m.group(0)) for m in tail]
    return desc, vals


# ===========================
# Helpers news
# ===========================

def _norm_u(s: Optional[str]) -> str:
    """Mayúsculas, sin acentos y espacios compactados (no elimina espacios)."""
    if not s:
        return ""
    u = s.upper()
    # quitar acentos/ñ
    u = (u.replace("Á", "A").replace("É", "E").replace("Í", "I")
           .replace("Ó", "O").replace("Ú", "U").replace("Ü", "U")
           .replace("Ñ", "N"))
    return re.sub(r"\s+", " ", u).strip()

def _has_tokens(desc: str, tokens: List[str]) -> bool:
    """
    True si 'desc' contiene alguno de los tokens, tolerando espacios/guiones.
    Checa dos variantes: con espacios y 'compacta' sin separadores.
    """
    u0 = _norm_u(desc)
    u1 = re.sub(r"[^A-Z0-9]", "", u0)  # sin espacios ni signos
    for t in tokens:
        t0 = _norm_u(t)
        t1 = re.sub(r"[^A-Z0-9]", "", t0)
        if t0 in u0 or t1 in u1:
            return True
    return False

# Palabras clave

# Créditos (abonos)
ABONO_KEYS = [
    "ABONO", "ABO ", "DEPOSITO", "RECIBIDO", "SPEI RECIBIDO",
    "INTERES", "INTERESES", "DEVOLUCION", "REEMBOLSO", "TRASPASO RECIBIDO"
]

# Débitos (retiros/cargos)
RETIRO_KEYS = [
    "RETIRO", "PAGO", "ENVIADO", "TRANSFERENCIA SPEI", "SPEI HORA", "SPEI ENVIADO",
    "COMISION", "IVA", "COMPRA", "COBRO",
    "RETENCION", "ISR", "MEMBRESIA", "MEMBRES", "CARGO", "CUOTA", "ANUALIDAD",
    "SEGURO", "SERVICIO", "MANTENIMIENTO",
    "APORT", "LINEA CAPTURA", "CAPTURA INTERNET"
]

# ===========================
# Clasificación por palabras (REEMPLAZA _classify_by_keywords completo)
# ===========================

def _classify_by_keywords(desc_upper: str, monto: float) -> Tuple[float, float]:
    """
    Devuelve (retiro, deposito) para un único monto cuando no sabemos la columna.
    """
    if monto == 0.0:
        return 0.0, 0.0
    # desc_upper puede venir ya en mayúsculas; normalizamos igual.
    if _has_tokens(desc_upper, ABONO_KEYS):
        return 0.0, monto
    if _has_tokens(desc_upper, RETIRO_KEYS):
        return monto, 0.0
    # por defecto, tratar como abono (depósito)
    return 0.0, monto

# ===========================
# Reconciliación (REEMPLAZA _reconcile_amounts completo)
# ===========================

def _reconcile_amounts(desc: str, dep: float, ret: float) -> Tuple[float, float]:
    """
    Un movimiento es o depósito o retiro. Usamos las palabras clave para
    desempatar y también para corregir cuando solo hay importe en una columna
    pero el texto indica lo contrario (p.ej. RETENCION/ISR, MEMBRESIA, CARGO).
    """
    dep = dep or 0.0
    ret = ret or 0.0
    has_abono = _has_tokens(desc, ABONO_KEYS)
    has_retiro = _has_tokens(desc, RETIRO_KEYS)

    # Caso: ambos llenos -> decide por keywords
    if dep > 0 and ret > 0:
        if has_abono and not has_retiro:
            ret = 0.0
        elif has_retiro and not has_abono:
            dep = 0.0
        else:
            # ambiguo, preferimos retiro para no inflar abonos
            dep = 0.0

    # Caso: solo hay depósito pero pinta a retiro -> muevelo a Retiro
    elif dep > 0 and ret == 0 and has_retiro and not has_abono:
        ret, dep = dep, 0.0

    # Caso: solo hay retiro pero pinta a abono -> muevelo a Depósitos
    elif ret > 0 and dep == 0 and has_abono and not has_retiro:
        dep, ret = ret, 0.0

    return round(dep, 2), round(ret, 2)


@dataclass
class TxRow:
    f_oper: Optional[date] = None
    folio: Optional[str] = None
    desc: str = ""
    deposito: float = 0.0
    retiro: float = 0.0
    saldo: Optional[float] = None


# ===========================
# Parsing por coordenadas
# ===========================

def _detect_columns(words: List[dict], page_width: float) -> Tuple[List[Tuple[float, float]], float]:
    hits: Dict[str, List[dict]] = {}
    labels = {
        "FECHA": ["FECHA"],
        "FOLIO": ["FOLIO"],
        "DESCRIPCION": ["DESCRIPCION", "DESCRIPCIÓN"],
        "DEPOSITO": ["DEPOSITO", "DEPÓSITO"],
        "RETIRO": ["RETIRO"],
        "SALDO": ["SALDO"],
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

    x_fecha = cx("FECHA", 0.07)
    x_folio = cx("FOLIO", 0.16)
    x_desc  = cx("DESCRIPCION", 0.40)
    x_dep   = cx("DEPOSITO", 0.62)
    x_ret   = cx("RETIRO",   0.74)
    x_saldo = cx("SALDO",    0.88)

    xs = sorted([x_fecha, x_folio, x_desc, x_dep, x_ret, x_saldo])
    def mid(a, b): return (a + b) / 2

    bounds = [
        (0,                 mid(xs[0], xs[1]) - 1),
        (mid(xs[0], xs[1]) + 1, mid(xs[1], xs[2]) - 1),
        (mid(xs[1], xs[2]) + 1, mid(xs[2], xs[3]) - 1),
        (mid(xs[2], xs[3]) + 1, mid(xs[3], xs[4]) - 1),
        (mid(xs[3], xs[4]) + 1, mid(xs[4], xs[5]) - 1),
        (mid(xs[4], xs[5]) + 1, page_width),
    ]

    header_y = 0.0
    if hits:
        header_y = max(min(v, key=lambda x: x["top"])["bottom"] for v in hits.values())
    return bounds, header_y


def _group_rows(words: List[dict], header_y: float) -> List[List[dict]]:
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
    cols = [""] * 6
    for w in row_words:
        xc = (w["x0"] + w["x1"]) / 2
        text = w["text"]
        for i, (x0, x1) in enumerate(bounds):
            if x0 <= xc <= x1:
                cols[i] = (cols[i] + " " + text).strip() if cols[i] else text
                break
    return [c.strip() for c in cols]


# ===========================
# Detección de encabezados por renglón
# ===========================

def _row_section_marker(row_words: List[dict]) -> Optional[str]:
    t = " ".join(w["text"] for w in row_words).upper()
    if ("DETALLE" in t and "MOVIMIENTOS" in t and "CUENTA" in t and ("CHEQUE" in t or "CHEQUES" in t)) \
       or "CUENTA SANTANDER PYME" in t:
        return "CUENTA"
    if ("DETALLE" in t and "MOVIMIENTOS" in t and "DINERO" in t and "CRECIENTE" in t) \
       or "INVERSION CRECIENTE" in t:
        return "INVER"
    return None


# ===========================
# Limpieza de DataFrames
# ===========================

def _clean_movements_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    df = df.copy()
    for c in ["Depósitos", "Retiro", "Saldo"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    mask_empty = (
        df["Fecha"].isna() &
        df["Folio"].fillna("").eq("") &
        df["Descripción"].fillna("").eq("") &
        df[["Depósitos", "Retiro", "Saldo"]].fillna(0).eq(0).all(axis=1)
    )
    df = df[~mask_empty].reset_index(drop=True)

    drop_mask = (
        df["Descripción"].astype(str).str.contains(INFO_FISCAL_PAT, regex=True) |
        df["Descripción"].astype(str).str.contains(PAGINA_WORD_PAT, regex=True) |
        df["Descripción"].astype(str).str.contains(DETALLES_HEADER_PAT, regex=True) |
        df["Descripción"].astype(str).str.contains(PP_NUM_PAT, regex=True) |
        (
            df["Descripción"].astype(str).str.contains(BASE64ISH_PAT, regex=True) &
            ~df["Descripción"].astype(str).str.contains(CLAVE_RASTREO_PAT, regex=True)
        )
    )

    df = df[~drop_mask].reset_index(drop=True)

    df["Descripción"] = df["Descripción"].astype(str).apply(_strip_page_garbage)
    return df


# ===========================
# Metadatos (Empresa desde 1ª página)
# ===========================

_RAZON_SUFFIX = (
    r"(?:S\.?\s*A\.?(?:\s*P\.?\s*I\.?)?\s*DE\s*C\.?\s*V\.?|"
    r"S\.?\s*DE\s*R\.?\s*L\.?(?:\s*DE\s*C\.?\s*V\.?)?|"
    r"A\.?\s*C\.?|S\.?\s*C\.?)"
)
# 1) match estricto: línea compuesta solo por texto + sufijo
_RAZON_LINE_RE = re.compile(rf"^[A-ZÁÉÍÓÚÑ&\.\-,' ]+\s{_RAZON_SUFFIX}$", re.I)
# 2) match amplio: cualquier segmento que termine en el sufijo (para buscar en texto completo)
_RAZON_ANY_RE  = re.compile(rf"([A-ZÁÉÍÓÚÑ&\.\-,' ]{{6,}}?\s{_RAZON_SUFFIX})", re.I)

_ADDR_TOKENS = {
    " CALLE"," AV "," AV."," AVENIDA"," PISO "," NUM "," NO."," N°"," Nº",
    " COL "," COLONIA"," C.P"," CP",
    " MÉXICO"," MEXICO"," ESTADO"," MUNICIP"," DELEGACIÓN"," ALCALDIA"," ALCALDÍA"," BARRIO",
    " SAN "," SANTA "," MIGUEL"," HIDALGO"," METEPEC"," CP."
}
_BLACKLIST = {
    "CONCEPTO", "FACTURA", "COMPROBANTE", "ESTADO DE CUENTA", "DETALLE",
    "MOVIMIENTOS", "PERIODO", "PERÍODO", "CUENTA", "SANTANDER", "BANCO"
}

def _limpia_espacios(u: str) -> str:
    return re.sub(r"\s+", " ", u).strip()

def _es_candidata_empresa(u: str) -> bool:
    u = " " + u.upper() + " "  # acolchonar para detectar tokens por palabra
    if any(ch.isdigit() for ch in u):
        return False
    if any(tok in u for tok in _ADDR_TOKENS):
        return False
    if any(bad in u for bad in _BLACKLIST):
        return False
    return True

def _empresa_desde_primera_pagina(pdf_path: str) -> Optional[str]:
    """
    Busca la razón social en el bloque superior-izquierdo de la 1ª página.
    Fallback 1: unión de dos líneas consecutivas.
    Fallback 2: regex amplio sobre el texto completo de la 1ª página.
    Fallback 3: línea mayúscula más larga de la región que no sea dirección/encabezado.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            p = pdf.pages[0]
            W, H = p.width, p.height
            words = p.extract_words(x_tolerance=2, y_tolerance=2, use_text_flow=True)

        # Región superior-izquierda (debajo del logo)
        region = [w for w in words if w["x0"] < W * 0.70 and w["top"] < H * 0.38]
        if region:
            # Agrupar por filas
            heights = [(w["bottom"] - w["top"]) for w in region]
            avg_h = (sum(heights) / len(heights)) if heights else 8.0
            ybin = max(2.0, min(4.2, avg_h * 0.70))
            lines: Dict[int, List[dict]] = {}
            for w in region:
                key = int(round(w["top"] / ybin))
                lines.setdefault(key, []).append(w)
            ordered_lines = [
                _limpia_espacios(" ".join(t["text"] for t in sorted(ws, key=lambda x: x["x0"])) )
                for _, ws in sorted(lines.items(), key=lambda kv: min(x["top"] for x in kv[1]))
            ]

            # Pass A: línea individual con sufijo típico
            for ln in ordered_lines[:15]:
                u = ln.upper()
                if _RAZON_LINE_RE.match(u) and _es_candidata_empresa(u):
                    return u

            # Pass B: unión de dos líneas (por si "SA DE CV" quedó abajo)
            for i in range(min(12, len(ordered_lines)-1)):
                u = _limpia_espacios((ordered_lines[i] + " " + ordered_lines[i+1])).upper()
                if _RAZON_LINE_RE.match(u) and _es_candidata_empresa(u):
                    return u

        # Fallback 2: regex amplio sobre todo el texto de la 1ª página
        with pdfplumber.open(pdf_path) as pdf:
            page_text = (pdf.pages[0].extract_text() or "").replace("\u00a0", " ")
        matches = _RAZON_ANY_RE.findall(page_text)
        cands = []
        for m in matches:
            u = _limpia_espacios(m.upper())
            if _es_candidata_empresa(u):
                cands.append(u)
        if cands:
            # La más larga suele ser la más completa
            return max(cands, key=len)

        # Fallback 3: toma la línea mayúscula más larga de la región (sin dígitos / tokens)
        if region:
            best = None
            for ln in ordered_lines[:20]:
                u = _limpia_espacios(ln.upper())
                if not EMPRESA_PAT.match(u):
                    continue
                if not _es_candidata_empresa(u):
                    continue
                if best is None or len(u) > len(best):
                    best = u
            if best:
                return best

        return None
    except Exception:
        return None


# ===========================
# Metadatos
# ===========================

def _parse_period(text: str) -> Tuple[Optional[date], Optional[date]]:
    m = PERIODO_PAT.search(text)
    if not m:
        return None, None
    d1 = _parse_ddmonyyyy(m.group(1))
    d2 = _parse_ddmonyyyy(m.group(2))
    return d1, d2


def _extract_header_metadata(full_text: str, pdf_path: str) -> Dict[str, Optional[str]]:
    txt = full_text.replace("\u00a0", " ")
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]

    # 1) Primero: razón social desde la 1ª página por coordenadas
    empresa = _empresa_desde_primera_pagina(pdf_path)

    # 2) Fallback: mismas reglas sobre el texto completo
    if not empresa:
        for ln in lines[:120]:
            u = re.sub(r"\s+", " ", ln).upper().strip()
            if _es_candidata_empresa(u):
                empresa = u
                break

    rfc = RFC_PAT.search(txt).group(1).upper() if RFC_PAT.search(txt) else None
    ncliente = CLIENTE_PAT.search(txt).group(1) if CLIENTE_PAT.search(txt) else None
    clabe = CLABE_PAT.search(txt).group(1) if CLIENTE_PAT.search(txt) else None
    pstart, pend = _parse_period(txt)

    return {
        "empresa": empresa,
        "rfc": rfc,
        "ncliente": ncliente,
        "clabe": clabe,
        "pstart": pstart,
        "pend": pend,
        "archivo": Path(pdf_path).name,
    }


# ===========================
# Extractor principal
# ===========================

def extract_santander_to_xlsx(pdf_path: str, out_xlsx: str) -> None:
    # 1) Texto completo para metadatos
    with pdfplumber.open(pdf_path) as pdf:
        all_text = "\n".join((p.extract_text() or "") for p in pdf.pages).replace("\u00a0", " ")
    meta_doc = _extract_header_metadata(all_text, pdf_path)
    pstart = meta_doc["pstart"]; pend = meta_doc["pend"]

    # 2) Productos y saldos anteriores (crudo)
    cuentas_raw: List[Dict[str, Optional[str]]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            pt = (page.extract_text() or "").replace("\u00a0", " ")
            if HEAD_CUENTA.search(pt) or PROD_CUENTA.search(pt):
                mnum = PROD_CUENTA.search(pt)
                msaldo = SALDO_ANT_PAT.search(pt)
                cuentas_raw.append({
                    "producto": "CUENTA SANTANDER PYME",
                    "numero": mnum.group(1) if mnum else None,
                    "clabe": meta_doc["clabe"],
                    "saldo_ant": _norm_amount(msaldo.group(1)) if msaldo else None,
                })
            if HEAD_INVER.search(pt) or PROD_INVER.search(pt):
                mnum = PROD_INVER.search(pt)
                msaldo = SALDO_ANT_PAT.search(pt)
                cuentas_raw.append({
                    "producto": "INVERSION CRECIENTE",
                    "numero": mnum.group(1) if mnum else None,
                    "clabe": None,
                    "saldo_ant": _norm_amount(msaldo.group(1)) if msaldo else None,
                })

    # --- Merge de cuentas por *producto*
    def _merge_accounts(crudo: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
        merged: Dict[str, Dict[str, Optional[str]]] = {}
        for c in crudo:
            prod = c["producto"]
            acc = merged.get(prod, {"producto": prod, "numero": None, "clabe": None, "saldo_ant": None})
            if not acc["numero"] and c.get("numero"):
                acc["numero"] = c["numero"]
            if prod == "CUENTA SANTANDER PYME":
                acc["clabe"] = meta_doc["clabe"]
            else:
                acc["clabe"] = None
            if acc["saldo_ant"] is None and c.get("saldo_ant") is not None:
                acc["saldo_ant"] = c["saldo_ant"]
            merged[prod] = acc

        out = []
        for prod in ("CUENTA SANTANDER PYME", "INVERSION CRECIENTE"):
            if prod in merged:
                out.append(merged[prod])
            else:
                out.append({
                    "producto": prod,
                    "numero": None,
                    "clabe": meta_doc["clabe"] if prod == "CUENTA SANTANDER PYME" else None,
                    "saldo_ant": None,
                })
        return out

    cuentas = _merge_accounts(cuentas_raw)

    # 3) Parseo de movimientos (cambio de sección dentro de la misma página)
    movs_cheques: List[TxRow] = []
    movs_inver:   List[TxRow] = []

    def flush_current(cur: Optional[TxRow], section: Optional[str]) -> None:
        if not cur or not section:
            return
        cur.deposito, cur.retiro = _reconcile_amounts(cur.desc, cur.deposito, cur.retiro)
        (movs_cheques if section == "CUENTA" else movs_inver).append(cur)

    current: Optional[TxRow] = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Determinar la sección SOLO por el primer encabezado visible en los renglones
            in_section: Optional[str] = None

            words_all = page.extract_words(x_tolerance=2, y_tolerance=2, use_text_flow=True)
            words = _drop_footer_totals(words_all)
            bounds, header_y = _detect_columns(words, page.width)
            rows = _group_rows(words, header_y)

            # Buscar encabezados dentro de la página (con orden visual)
            markers: Dict[int, str] = {}
            for idx, r in enumerate(rows):
                sec = _row_section_marker(r)
                if sec:
                    markers[idx] = sec

            # Primera marca manda; si no hay, por defecto 'CUENTA'
            if markers:
                in_section = markers[min(markers.keys())]
            else:
                in_section = "CUENTA"

            for i, r in enumerate(rows):
                if i in markers:
                    flush_current(current, in_section)
                    current = None
                    in_section = markers[i]    # cambia sección al vuelo por encabezado
                    continue

                cols = _assign_cols(r, bounds)
                fecha_raw, folio_raw, desc_raw, dep_raw, ret_raw, saldo_raw = cols
                row_text = " ".join(w["text"] for w in r)
                row_up   = row_text.upper()

                # Saltos obvios
                if all(tok in row_up for tok in ("FECHA", "FOLIO", "SALDO")):  continue
                if row_up.strip().startswith("TOTAL"):                         continue
                if "SALDO FINAL DEL PERIODO" in row_up:                        continue
                if DETALLES_HEADER_PAT.search(row_up):                         continue
                if PAGINA_WORD_PAT.search(row_up) or PP_NUM_PAT.search(row_up): continue

                new_date  = bool(DATE_TOKEN_SANT.search(fecha_raw))
                has_money = any(AMOUNT_RE.search(x or "") for x in (dep_raw, ret_raw, saldo_raw))
                no_money  = not has_money

                if new_date:
                    flush_current(current, in_section); current = None
                    f = _parse_ddmonyyyy(fecha_raw)

                    line_wo_date = DATE_TOKEN_SANT.sub("", row_text, count=1).strip()
                    line_wo_date = _cut_after_totals(line_wo_date)
                    line_wo_date = re.sub(r"^\d{5,}\s+", "", line_wo_date)
                    desc_fb, tail = _extract_tail_run(line_wo_date, 3)

                    dep_fb = ret_fb = 0.0; sal_fb = None
                    if len(tail) == 3:
                        dep_fb, ret_fb, sal_fb = tail
                    elif len(tail) == 2:
                        wv, dv = _classify_by_keywords(desc_fb.upper(), tail[0]); ret_fb, dep_fb = wv, dv; sal_fb = tail[1]
                    elif len(tail) == 1:
                        if _has_total_or_final(row_text):
                            wv, dv = _classify_by_keywords(desc_fb.upper(), tail[0])
                            ret_fb, dep_fb = wv, dv
                        else:
                            sal_fb = tail[0]

                    dep_col = _pick_amount(dep_raw, "first")
                    ret_col = _pick_amount(ret_raw, "first")
                    sal_col = _pick_amount(saldo_raw, "last")
                    if _has_total_or_final(row_text):
                        dep_col = ret_col = sal_col = None
                    if len(tail) >= 2:
                        dep_col = ret_col = sal_col = None

                    desc_txt = _strip_page_garbage((desc_fb or (desc_raw or "")).strip())
                    if _is_garbage_piece(desc_txt):
                        desc_txt = ""

                    dep_in = dep_col if dep_col is not None else (dep_fb or 0.0)
                    ret_in = ret_col if ret_col is not None else (ret_fb or 0.0)
                    dep_v, ret_v = _reconcile_amounts(desc_txt, dep_in, ret_in)

                    current = TxRow(
                        f_oper=f,
                        folio=folio_raw.strip() or None,
                        desc=desc_txt,
                        deposito=dep_v,
                        retiro=ret_v,
                        saldo=sal_col if sal_col is not None else sal_fb,
                    )
                    continue

                if current and no_money and not new_date and (desc_raw or folio_raw or fecha_raw):
                    piece_col = (desc_raw or "").strip()
                    line = re.sub(r"^\d{5,}\s+", "", row_text.strip())
                    line = _cut_after_totals(line)
                    piece_fb, _ = _extract_tail_run(line, 3)
                    piece = _strip_page_garbage(piece_fb if len(piece_fb) > len(piece_col) else piece_col)
                    if piece and not _is_garbage_piece(piece):
                        current.desc = (current.desc + " " + piece).strip()
                    continue

                if current and has_money and not new_date:
                    text_for_tail = _cut_after_totals(row_text)
                    desc_fb, tail = _extract_tail_run(text_for_tail, 3)

                    dep_v = ret_v = None; sal_v = None
                    if len(tail) == 3:
                        dep_v, ret_v, sal_v = tail
                    elif len(tail) == 2:
                        wv, dv = _classify_by_keywords((desc_fb or current.desc).upper(), tail[0])
                        ret_v, dep_v = wv, dv; sal_v = tail[1]
                    elif len(tail) == 1:
                        if _has_total_or_final(row_text):
                            wv, dv = _classify_by_keywords((desc_fb or current.desc).upper(), tail[0])
                            ret_v, dep_v = wv, dv
                        else:
                            sal_v = tail[0]

                    dep_col = _pick_amount(dep_raw, "first")
                    ret_col = _pick_amount(ret_raw, "first")
                    sal_col = _pick_amount(saldo_raw, "last")

                    if len(tail) >= 2:
                        dep_col = ret_col = sal_col = None
                    if _has_total_or_final(row_text):
                        dep_col = ret_col = sal_col = None

                    if dep_col is not None or dep_v is not None:
                        current.deposito = dep_col if dep_col is not None else (dep_v or 0.0)
                    if ret_col is not None or ret_v is not None:
                        current.retiro   = ret_col if ret_col is not None else (ret_v or 0.0)
                    if sal_col is not None or sal_v is not None:
                        current.saldo    = sal_col if sal_col is not None else sal_v

                    current.deposito, current.retiro = _reconcile_amounts(current.desc, current.deposito, current.retiro)

                    piece = _strip_page_garbage((desc_fb or desc_raw or "").strip())
                    if piece and not _is_garbage_piece(piece):
                        current.desc = (current.desc + " " + piece).strip()

                    continue

            flush_current(current, in_section)
            current = None
            # No arrastrar sección a la siguiente página; se recalcula cada una
            in_section = None

    # 4) DataFrames de movimientos
    def _txs_to_df(txs: List[TxRow]) -> pd.DataFrame:
        def fmt(d: Optional[date]) -> Optional[str]:
            return d.strftime("%d-%m-%Y") if isinstance(d, date) else None
        df = pd.DataFrame([{
            "Fecha": fmt(t.f_oper),
            "Folio": t.folio,
            "Descripción": re.sub(r"\s+", " ", (t.desc or "")).strip(),
            "Depósitos": round(t.deposito or 0.0, 2),
            "Retiro": round(t.retiro or 0.0, 2),
            "Saldo": round(t.saldo, 2) if t.saldo is not None else None,
        } for t in txs], columns=["Fecha", "Folio", "Descripción", "Depósitos", "Retiro", "Saldo"])
        return _clean_movements_df(df)

    movs_df_cheques = _txs_to_df(movs_cheques)
    movs_df_inver   = _txs_to_df(movs_inver)

    # --- Relleno del "Saldo final del periodo anterior"
    def _infer_opening(df: pd.DataFrame) -> Optional[float]:
        if df.empty:
            return None
        r0 = df.iloc[0]
        sal = r0.get("Saldo")
        dep = r0.get("Depósitos") or 0.0
        ret = r0.get("Retiro") or 0.0
        try:
            return round(float(sal) - float(dep) + float(ret), 2) if pd.notna(sal) else None
        except Exception:
            return None

    for c in cuentas:
        if c["saldo_ant"] is None:
            if c["producto"] == "CUENTA SANTANDER PYME":
                c["saldo_ant"] = _infer_opening(movs_df_cheques)
            elif c["producto"] == "INVERSION CRECIENTE":
                c["saldo_ant"] = _infer_opening(movs_df_inver) or 0.0

    # 5) Hojas info y cuenta
    info_df = pd.DataFrame([{
        "Banco": "SANTANDER",
        "Archivo": meta_doc["archivo"],
        "Periodo inicio": pstart.strftime("%d-%m-%Y") if pstart else None,
        "Periodo fin":    pend.strftime("%d-%m-%Y") if pend else None,
        "Empresa": meta_doc["empresa"],
        "RFC": meta_doc["rfc"],
    }])

    cuenta_rows = []
    for c in cuentas:
        cuenta_rows.append({
            "No. de cuenta": c.get("numero"),
            "No. Cliente": meta_doc["ncliente"],
            "CLABE": c.get("clabe"),
            "Producto": c.get("producto"),
            "Moneda": "MXN",
            "Saldo final del periodo anterior": c.get("saldo_ant"),
        })
    cuenta_df = pd.DataFrame(cuenta_rows, columns=[
        "No. de cuenta", "No. Cliente", "CLABE", "Producto", "Moneda",
        "Saldo final del periodo anterior"
    ])

    # 6) Excel
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        info_df.to_excel(xl, index=False, sheet_name="info")
        cuenta_df.to_excel(xl, index=False, sheet_name="cuenta")
        movs_df_cheques.to_excel(xl, index=False, sheet_name="movimientos_cuenta")
        movs_df_inver.to_excel(xl, index=False, sheet_name="movimientos_inversion")


# CLI opcional
if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        extract_santander_to_xlsx(sys.argv[1], sys.argv[2])
    else:
        print("Uso: python santander_extractor.py input.pdf salida.xlsx")
