# inbursa_extractor.py
from __future__ import annotations
import os, re, math, unicodedata
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pdfplumber
import pandas as pd

# ------------------------------ Utils ------------------------------
def _clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _norm(s: str | None) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.upper()

SPAN_MONTHS = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12,
}
DATE_TOKEN = r"(?:ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)"
RX_MON_ONLY = re.compile(rf"^\s*({DATE_TOKEN})[\.,]?\s*$", re.I)
RX_MON_DAY  = re.compile(rf"^\s*({DATE_TOKEN})[\.,]?\s+(\d{{1,2}})\s*$", re.I)
RX_DAY_ONLY = re.compile(r"^\s*(\d{1,2})\s*$")

AMOUNT_RX = re.compile(r"\d{1,3}(?:,\d{3})*\.\d{2}")
ALPHA_RX  = re.compile(r"[A-Za-zÁÉÍÓÚÑáéíóúñ]")
REF10_RX  = re.compile(r"\b(\d{10})\b")

def _to_amount(s: str | None) -> Optional[float]:
    if not s:
        return None
    m = AMOUNT_RX.search(s)
    return float(m.group(0).replace(",", "")) if m else None

# ---------------- Encabezados (info / cuenta) ----------------
def _find_period(full_text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(
        r"PERIODO\s+Del\s+(\d{1,2})\s+([A-Za-zÁÉÍÓÚÑ]{3,})\.?,?\s+(\d{4})\s+al\s+"
        r"(\d{1,2})\s+([A-Za-zÁÉÍÓÚÑ]{3,})\.?,?\s+(\d{4})",
        full_text, re.I
    )
    if not m:
        return None, None
    d1, m1, y1, d2, m2, y2 = m.groups()

    def _mk(d, M, y):
        abbr = _norm(M)[:3]
        mm = SPAN_MONTHS.get(abbr)
        if not mm:
            return None
        return f"{int(d):02d}-{mm:02d}-{int(y):04d}"

    return _mk(d1, m1, y1), _mk(d2, m2, y2)

def _parse_header_info(full_text: str) -> Dict[str, Optional[str]]:
    m_acc = re.search(r"\bCUENTA\s+(\d{6,})", full_text, re.I)
    m_cla = re.search(r"\bCLABE\s+(\d{18})", full_text, re.I)
    m_cur = re.search(r"\bMONEDA\s+([A-Z]{3})", full_text, re.I)
    m_cli = re.search(r"Cliente\s+Inbursa:\s*(\d+)", full_text, re.I)
    m_rfc = re.search(r"\bRFC:\s*([A-Z0-9]{12,13})", full_text, re.I)
    p_ini, p_fin = _find_period(full_text)
    return {
        "account": m_acc.group(1) if m_acc else None,
        "clabe": m_cla.group(1) if m_cla else None,
        "moneda": (m_cur.group(1) if m_cur else "MXN"),
        "cliente": m_cli.group(1) if m_cli else None,
        "periodo_inicio": p_ini,
        "periodo_fin": p_fin,
        "rfc": m_rfc.group(1) if m_rfc else None,
    }

def _guess_year(info: Dict[str, Optional[str]]) -> int:
    for k in ("periodo_fin", "periodo_inicio"):
        v = info.get(k)
        if v:
            return int(v[-4:])
    return datetime.now().year

# ---------------- Detección de columnas y filas ----------------
FOOTER_TERMS = [
    "RESUMEN GRAFICO", "TIPO COMPROBANTE", "EXPRESADAS EN TERMINOS ANUALES",
    "RENDIMIENTOS", "BANCO INBURSA", "REGIMEN FISCAL", "CLIENTE INBURSA"
]

def _detect_header_centers(page) -> tuple[Optional[Dict[str, float]], float]:
    words = page.extract_words(x_tolerance=2.8, y_tolerance=2.8, use_text_flow=False) or []
    if not words:
        return None, 0.0

    targets = {"FECHA","REFERENCIA","CONCEPTO","DESCRIPCION","CARGOS","ABONOS","SALDO"}

    y_bin = 6
    byline: Dict[int, List[dict]] = {}
    for w in words:
        key = round(w["top"] / y_bin) * y_bin
        byline.setdefault(key, []).append(w)

    best_key, best_hits = None, -1
    for k, ws in byline.items():
        labels = { _norm(w["text"]) for w in ws }
        has_core = ("FECHA" in labels) and ("CONCEPTO" in labels or "DESCRIPCION" in labels)
        if not has_core:
            continue
        hits = sum(1 for t in targets if t in labels)
        if hits > best_hits:
            best_hits, best_key = hits, k

    if best_key is None:
        return None, 0.0

    header_ws = byline[best_key]
    centers: Dict[str, float] = {}
    for w in header_ws:
        t = _norm(w["text"])
        if t in targets and t not in centers:
            centers[t if t != "DESCRIPCION" else "CONCEPTO"] = (w["x0"] + w["x1"]) / 2

    if "CONCEPTO" in centers and "SALDO" in centers:
        x_conc, x_saldo = centers["CONCEPTO"], centers["SALDO"]
        if "CARGOS" not in centers and "ABONOS" not in centers:
            centers["CARGOS"] = x_conc + (x_saldo - x_conc) / 3
            centers["ABONOS"] = x_conc + 2 * (x_saldo - x_conc) / 3
        elif "CARGOS" not in centers:
            centers["CARGOS"] = (x_conc + centers["ABONOS"]) / 2
        elif "ABONOS" not in centers:
            centers["ABONOS"] = (centers["CARGOS"] + x_saldo) / 2

    y_cut = max(w["bottom"] for w in header_ws) + 0.5
    return centers, y_cut

def _build_xbands(centers: Dict[str, float]) -> Dict[str, tuple[float,float]]:
    cols = sorted(centers.items() , key=lambda kv: kv[1])
    keys = [k for k,_ in cols]
    xs   = [x for _,x in cols]
    bands: Dict[str, tuple[float,float]] = {}
    for i, k in enumerate(keys):
        left  = -math.inf if i == 0 else (xs[i-1] + xs[i]) / 2
        right =  math.inf if i == len(keys)-1 else (xs[i] + xs[i+1]) / 2
        bands[k] = (left, right)
    return bands

def _bucket_by_xbands(line_words: List[dict], bands: Dict[str, tuple[float,float]]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {k: [] for k in bands.keys()}
    for w in sorted(line_words, key=lambda x: x["x0"]):
        xmid = (w["x0"] + w["x1"]) / 2
        for k, (lx, rx) in bands.items():
            if lx <= xmid < rx:
                out[k].append(w)
                break

    # REFERENCIA → deja sólo el ref de 10 dígitos; lo demás va a CONCEPTO
    if "REFERENCIA" in out and "CONCEPTO" in out:
        keep_ref, move_ref = [], []
        for w in out["REFERENCIA"]:
            t = _clean(w["text"])
            if re.fullmatch(r"\d{10}", t):
                keep_ref.append(w)
            else:
                move_ref.append(w)
        out["REFERENCIA"] = keep_ref
        out["CONCEPTO"].extend(move_ref)

    # FECHA → mueve palabras/números largos que no son fecha real a CONCEPTO
    if "FECHA" in out and "CONCEPTO" in out:
        keep_f, move_f = [], []
        for w in out["FECHA"]:
            t = _clean(w["text"]); n = _norm(t)
            if RX_MON_DAY.match(n) or RX_MON_ONLY.match(n) or RX_DAY_ONLY.match(n):
                keep_f.append(w)
            elif ALPHA_RX.search(t) or re.fullmatch(r"\d{5,}", t):
                move_f.append(w)
            else:
                keep_f.append(w)
        out["FECHA"] = keep_f
        out["CONCEPTO"].extend(move_f)

    return out

# ---------------- Normalización de “Descripción” ----------------
INTERESES_RX = re.compile(r"\b(INTERESES\s+GANADOS|GANADOS\s+INTERESES)\b", re.I)
BBVA_AFTER_MEXICO_RX = re.compile(r"\bMEXICO\s+(\d{6,})\s+BBVA\b", re.I)
EF_CORR_RX = re.compile(r"(?i)(EFECTIVO\s+CORRESPONSAL)\s+(Monterrey\s+NL\s+MX)\s+(Edison\s+\d+)")
DOCTORES_MAYO_SWAP_RX = re.compile(r"(?i)\b(Doctores\s+Mayo)\s+(\d{4,})\b")
BANK_NUMBER_FLIP_RX = re.compile(
    r"(?i)\b(\d{6,})\s+(BANAMEX|CITIBANAMEX|AZTECA|HSBC|SANTANDER|SCOTIABANK|BANORTE|BBVA(?:\s+MEXICO)?|NU(?:\s+MEXICO)?)\b"
)
TRACK_CODE_RX = re.compile(r"\b(MBAN[0-9A-Z]+|NU[0-9A-Z]+|\d{15,20})\b", re.I)

def _inject_clave_rastreo(s: str) -> str:
    if not s:
        return s
    s = re.sub(r"(?i)\bDE\s+RASTREO\b", "", s)
    s = re.sub(r"(?i)(CLAVE\s+DE\s+RASTREO\s+\S+)(?:\s+\1)+", r"\1", s)
    if re.search(r"(?i)\bCLAVE\s+DE\s+RASTREO\b", s):
        s = re.sub(r"(?i)\bCLAVE\s*$", "", s)
        return _clean(s)
    last = None
    for m in TRACK_CODE_RX.finditer(s):
        last = m
    if last:
        s = s[:last.start()] + " CLAVE DE RASTREO " + s[last.start():]
    s = re.sub(r"(?i)\bCLAVE\s*$", "", s)
    return _clean(s)

def _fix_bank_number_order(s: str) -> str:
    if not s:
        return s
    s = BANK_NUMBER_FLIP_RX.sub(lambda m: f"{m.group(2)} {m.group(1)}", s)
    s = re.sub(r"(?i)\bNU\s+(?=\d)", "NU MEXICO ", s)
    return _clean(s)

def _normalize_concept_lines(lines: List[str]) -> str:
    s = _clean(" ".join(_clean(x) for x in lines if _clean(x)))

    if INTERESES_RX.search(s):
        return "INTERESES GANADOS"

    s = BBVA_AFTER_MEXICO_RX.sub(r"BBVA MEXICO \1", s)
    s = EF_CORR_RX.sub(r"\1 \3 \2", s)
    s = DOCTORES_MAYO_SWAP_RX.sub(r"\2 \1", s)
    s = _fix_bank_number_order(s)
    s = _inject_clave_rastreo(s)
    s = re.sub(r"(?i)^\s*(DEPOSITO)\s+(?=\1\b)", r"\1 ", s)

    return _clean(s)

ROW_START_RX = re.compile(
    r"^\s*(?:DEPOSITO\s+(?:SPEI|EFECTIVO\s+CORRESPONSAL)|INTERESES\s+GANADOS|BALANCE\s+INICIAL)\b",
    re.I
)

# ---------------- Parse de una página ----------------
def _parse_page(page, year: int) -> List[Dict]:
    centers, y_cut = _detect_header_centers(page)
    if not centers:
        return []

    words = page.extract_words(x_tolerance=2.8, y_tolerance=2.8, use_text_flow=False) or []
    below = [w for w in words if w["top"] >= y_cut]

    y_bin = 3
    lines: Dict[int, List[dict]] = {}
    for w in below:
        key = round(w["top"] / y_bin) * y_bin
        lines.setdefault(key, []).append(w)

    xb = _build_xbands(centers)

    def jtxt(ws: List[dict]) -> str:
        return _clean(" ".join(w["text"] for w in sorted(ws, key=lambda x: x["x0"])))

    def jlines(ws: List[dict]) -> List[str]:
        yb = 2
        by: Dict[int, List[dict]] = {}
        for w in ws:
            k = round(w["top"] / yb) * yb
            by.setdefault(k, []).append(w)
        out = []
        for _, group in sorted(by.items()):
            out.append(_clean(" ".join(w["text"] for w in sorted(group, key=lambda x: x["x0"]))))
        return out

    def looks_like_footer(full_line: List[dict]) -> bool:
        t = _norm(" ".join(w["text"] for w in sorted(full_line, key=lambda x: x["x0"])))
        return any(term in t for term in FOOTER_TERMS)

    rows: List[Dict] = []

    last_month: Optional[str] = None
    pending_month: Optional[str] = None

    cur_date: Optional[str] = None
    active_date: Optional[str] = None
    cur_desc_lines: List[str] = []
    cur_ref: Optional[str] = None
    cur_cargo: Optional[float] = None
    cur_abono: Optional[float] = None
    cur_saldo: Optional[float] = None

    stash_desc_lines: List[str] = []
    stash_ref: Optional[str] = None
    stash_cargo: Optional[float] = None
    stash_abono: Optional[float] = None
    stash_saldo: Optional[float] = None

    def flush(force=False):
        nonlocal cur_date, cur_desc_lines, cur_ref, cur_cargo, cur_abono, cur_saldo
        if cur_date and (force or cur_desc_lines or any(v is not None for v in (cur_cargo, cur_abono, cur_saldo))):
            desc = _normalize_concept_lines(cur_desc_lines)
            rows.append({
                "Fecha": cur_date,
                "Referencia": cur_ref or "",
                "Descripción": desc,
                "Cargos": cur_cargo if cur_cargo is not None else "",
                "Abonos": cur_abono if cur_abono is not None else "",
                "Saldo":  cur_saldo if cur_saldo is not None else "",
            })
        cur_date = None; cur_desc_lines = []; cur_ref = None
        cur_cargo = None; cur_abono = None; cur_saldo = None

    for _, line in sorted(lines.items()):
        if looks_like_footer(line):
            flush(force=True)
            break

        buckets = _bucket_by_xbands(line, xb)

        fecha_txt = jtxt(buckets.get("FECHA", []))
        ref_txt   = jtxt(buckets.get("REFERENCIA", []))
        conc_ws   = buckets.get("CONCEPTO", [])
        conc_lines = jlines(conc_ws)
        car_txt   = jtxt(buckets.get("CARGOS", [])) if "CARGOS" in xb else ""
        abo_txt   = jtxt(buckets.get("ABONOS", [])) if "ABONOS" in xb else ""
        sal_txt   = jtxt(buckets.get("SALDO",  [])) if "SALDO"  in xb else ""

        # Fecha
        new_date: Optional[str] = None
        if fecha_txt:
            n = _norm(fecha_txt).replace(",", " ").strip()
            m = RX_MON_DAY.match(n)
            if m:
                mon, day = m.groups()
                last_month = mon; pending_month = None
                new_date = f"{int(day):02d}-{SPAN_MONTHS[mon]:02d}-{year:04d}"
            else:
                m1 = RX_MON_ONLY.match(n)
                if m1:
                    pending_month = m1.group(1); last_month = pending_month
                else:
                    m2 = RX_DAY_ONLY.match(n)
                    if m2 and (pending_month or last_month):
                        mon = pending_month or last_month
                        pending_month = None
                        new_date = f"{int(m2.group(1)):02d}-{SPAN_MONTHS[mon]:02d}-{year:04d}"

        if new_date:
            flush()
            cur_date = new_date
            active_date = new_date
            if stash_desc_lines:
                cur_desc_lines.extend(stash_desc_lines); stash_desc_lines = []
            if stash_ref and not cur_ref: cur_ref = stash_ref; stash_ref = None
            if stash_cargo is not None: cur_cargo = stash_cargo; stash_cargo = None
            if stash_abono is not None: cur_abono = stash_abono; stash_abono = None
            if stash_saldo is not None: cur_saldo = stash_saldo; stash_saldo = None

        # Posible corte por nueva referencia o inicio de concepto
        ref_cand = None
        if ref_txt or conc_lines:
            m10 = REF10_RX.search(f"{ref_txt} {' '.join(conc_lines)}")
            if m10:
                ref_cand = m10.group(1)

        conc_first = next((ln for ln in conc_lines if _clean(ln)), "")
        starts_new = bool(ROW_START_RX.match(conc_first))

        if cur_date is not None:
            have_any = bool(cur_desc_lines or cur_ref or cur_cargo is not None or cur_abono is not None or cur_saldo is not None)
            ref_changes = ref_cand and ((cur_ref and ref_cand != cur_ref) or (cur_ref is None and have_any))
            if ref_changes or (starts_new and cur_desc_lines):
                flush()
                cur_date = active_date

        # Referencia
        if ref_cand:
            if cur_date is None:
                stash_ref = ref_cand
            elif not cur_ref:
                cur_ref = ref_cand

        # Descripción
        if conc_lines:
            if cur_date is None:
                stash_desc_lines.extend(conc_lines)
            else:
                cur_desc_lines.extend(conc_lines)

        # Montos
        v_cargo = _to_amount(car_txt)
        v_abono = _to_amount(abo_txt)
        v_saldo = _to_amount(sal_txt)

        if v_abono is None:
            nums = [float(x.replace(",", "")) for x in AMOUNT_RX.findall(sal_txt or "")]
            if len(nums) >= 2:
                v_abono = nums[-2]
                if v_saldo is None:
                    v_saldo = nums[-1]
        if v_cargo is None:
            nums = [float(x.replace(",", "")) for x in AMOUNT_RX.findall(sal_txt or "")]
            if len(nums) >= 2 and v_abono is None:
                v_cargo = nums[-2]
                if v_saldo is None:
                    v_saldo = nums[-1]

        if cur_date is None:
            if v_cargo is not None: stash_cargo = v_cargo
            if v_abono is not None: stash_abono = v_abono
            if v_saldo is not None: stash_saldo = v_saldo
        else:
            if v_cargo is not None: cur_cargo = v_cargo
            if v_abono is not None: cur_abono = v_abono
            if v_saldo is not None: cur_saldo = v_saldo

    flush()
    return rows

# --------- Posprocesado de DataFrame: unir filas huérfanas ---------
def _is_empty_val(v) -> bool:
    if v is None: return True
    if isinstance(v, float) and pd.isna(v): return True
    if isinstance(v, str) and not v.strip(): return True
    return False

def _merge_orphan_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si una fila no tiene Referencia NI Cargos/Abonos/Saldo,
    se considera “huérfana”: se concatena su Descripción al final
    de la fila inmediatamente anterior y se elimina.
    """
    cols = ["Fecha","Referencia","Descripción","Cargos","Abonos","Saldo"]
    out: List[Dict] = []
    for _, row in df.iterrows():
        ref_empty = _is_empty_val(row.get("Referencia"))
        no_amounts = all(_is_empty_val(row.get(c)) for c in ["Cargos","Abonos","Saldo"])
        desc = str(row.get("Descripción") or "").strip()
        if ref_empty and no_amounts and desc and len(out) > 0:
            prev = out[-1].copy()
            prev["Descripción"] = _clean(f"{prev.get('Descripción','')} {desc}")
            out[-1] = prev
        else:
            out.append({k: row.get(k) for k in cols})
    return pd.DataFrame(out, columns=cols)

# ---------------- Export principal ----------------
def extract_inbursa_to_xlsx(pdf_path: str, xlsx_out: str) -> None:
    pages_text: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for p in pdf.pages:
            pages_text.append(p.extract_text() or "")
        full_text = "\n".join(pages_text)

        hdr = _parse_header_info(full_text)
        year = _guess_year(hdr)

        info_df = pd.DataFrame([{
            "Banco": "INBURSA",
            "Archivo": os.path.basename(pdf_path),
            "Periodo inicio": hdr.get("periodo_inicio"),
            "Periodo fin": hdr.get("periodo_fin"),
            "Empresa": "",
            "RFC": hdr.get("rfc") or "",
        }])

        cuenta_df = pd.DataFrame([{
            "No. de cuenta": hdr.get("account") or "",
            "No. de cliente": hdr.get("cliente") or "",
            "CLABE": hdr.get("clabe") or "",
            "Producto": "Cuenta",
            "Moneda": hdr.get("moneda") or "MXN",
        }])

        all_rows: List[Dict] = []
        for page in pdf.pages:
            all_rows.extend(_parse_page(page, year))

    # DataFrame de movimientos
    mov_df = pd.DataFrame(all_rows, columns=["Fecha","Referencia","Descripción","Cargos","Abonos","Saldo"])
    for col in ["Cargos","Abonos","Saldo"]:
        mov_df[col] = pd.to_numeric(mov_df[col], errors="coerce")

    # ✅ Une “continuaciones” huérfanas
    mov_df = _merge_orphan_rows(mov_df)

    with pd.ExcelWriter(xlsx_out, engine="openpyxl") as xw:
        info_df.to_excel(xw, sheet_name="info", index=False)
        cuenta_df.to_excel(xw, sheet_name="cuenta", index=False)
        mov_df.to_excel(xw, sheet_name="movimientos", index=False)

# Ejemplo:
# extract_inbursa_to_xlsx("MAYO IMBURSA      EdoCuenta_Inbursa.pdf", "salida_inbursa.xlsx")
