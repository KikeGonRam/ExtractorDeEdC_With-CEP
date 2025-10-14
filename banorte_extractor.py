from __future__ import annotations

import re
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple, Dict
from pathlib import Path

import pdfplumber
import pandas as pd

# -----------------------------------------------------------
# Utilidades
# -----------------------------------------------------------

MES_ABREV = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}

# Importes “genéricos” con decimales (para búsquedas sueltas)
_AMOUNT_RE = re.compile(r"\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}")
# Token de importe para runs: acepta decimales o un cero “pelón”
_AMT_CORE = r"(?:\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}|0(?:\.0{1,2})?)"
# Run de 2 o 3 importes contiguos en cualquier parte del renglón
_AMT_RUN_RE = re.compile(rf"({_AMT_CORE}(?:\s+{_AMT_CORE}){{1,2}})")

_DATE_TOKEN = re.compile(r"(\d{2}-[A-Za-zÁá]{3}-\d{2})", re.IGNORECASE)

_HEADER_OR_FOOTER_PAT = re.compile(
    r"(?i)("
    r"línea\s+directa\s+para\s+su\s+empresa|"
    r"visita\s+nuestra\s+p[aá]gina|www\.banorte\.com|"
    r"banco\s+mercantil\s+del\s+norte|"
    r"estado\s+de\s+cuenta\s*/\s*fecha\s+descrip|"
    r"fechas?\s+descripci[oó]n\s*/\s*establecimiento.*?saldo|"
    r"ganancia\s+anual\s+total\s*\(gat\)|gat\s+nominal|advertencia:|"
    r"cuando\s+no\s+reciba\s+su\s+estado\s+de\s+cuenta"
    r")"
)

# Sentinelas de “texto parásito” que aparecen tras los movimientos
_STOP_TRAIL_PAT = re.compile(
    r"(?is)(?:\bOTROS\s*[▼>]|Folio\s+Fecha\s+Tipo\s+de\s+Cargo|"
    r"Referencia\s+de\s+Abreviaturas|COMPROBANTE\s+FISCAL\s+DIGITAL|"
    r"\bIPAB\b|www\.ipab\.org\.mx)"
)

def _norm_amount(s: Optional[str]) -> float:
    if not s:
        return 0.0
    txt = (s.replace("\u00a0", " ")
             .replace("$", "")
             .replace(" ", "")
             .replace(",", ""))
    try:
        return float(txt)
    except Exception:
        return 0.0

def _round2(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x), 2)

def _parse_fecha_es(s: str) -> date:
    s0 = s.strip().lower().replace("\u00a0", " ")
    m = re.match(r"^(\d{2})[-/ ]([a-zá]{3})[-/ ](\d{2})$", s0)
    if m:
        d = int(m.group(1))
        mes = MES_ABREV.get(m.group(2).replace("á", "a"))
        y = 2000 + int(m.group(3))
        if mes:
            return date(y, mes, d)
    raise ValueError(f"Fecha no reconocida: {s}")

def _classify(desc_upper: str, monto: float) -> Tuple[float, float]:
    """
    Decide si 'monto' es depósito o retiro con base en palabras clave.
    Nota: tratamos explícitamente ISR/I.S.R. como RETIRO.
    """
    if monto == 0.0:
        return 0.0, 0.0

    # Depósitos / abonos
    if any(k in desc_upper for k in [
        "SPEI RECIBIDO", "ABONO", "DEPÓSITO", "DEPOSITO", "INTERESES",
        "DEPOSITO DE CUENTA", "DE CUENTA DE TERCEROS (ABONO)"
    ]):
        return 0.0, monto

    # Retiros / cargos
    if any(k in desc_upper for k in [
        "RETIRO", "PAGO", "COMPRA", "COMISION", "COMISIÓN", "TRASPASO",
        "IVA", "I.V.A", "PAGO TERCEROS", "TRASPASO A CUENTA DE TERCEROS"
    ]) or re.search(r"\bI\.?S\.?R\b", desc_upper):
        #                          ^---- ISR / I.S.R
        return monto, 0.0

    # Si no reconocemos, por defecto lo tratamos como depósito
    return 0.0, monto

def _extract_amount_run(s: str) -> Tuple[str, List[float]]:
    """
    Devuelve (descripcion_sin_run, [montos...]) tomando el run (bloque)
    **más a la derecha** con 2 o 3 importes contiguos. Si no hay run,
    regresa el último importe aislado (si existe) como lista de 1.
    """
    s = s.rstrip()
    runs = list(_AMT_RUN_RE.finditer(s))
    if runs:
        m = max(runs, key=lambda x: x.end())
        run_txt = m.group(0)
        tokens = re.findall(_AMT_CORE, run_txt)
        desc = (s[:m.start()] + s[m.end():]).rstrip()
        return desc, [_norm_amount(t) for t in tokens]

    singles = list(re.finditer(_AMT_CORE, s))
    if singles:
        m = singles[-1]
        desc = (s[:m.start()] + s[m.end():]).rstrip()
        return desc, [_norm_amount(m.group(0))]

    return s, []

# -----------------------------------------------------------
# Encabezado + Resumen integral
# -----------------------------------------------------------

def _parse_header_info(full_text: str) -> Dict[str, Optional[str]]:
    empresa = None
    rfc = None
    no_cli = None

    lines = [ln.strip() for ln in full_text.splitlines()]
    for i, ln in enumerate(lines):
        m = re.search(r"\bRFC[: ]+([A-Z0-9]{12,13})\b", ln, re.IGNORECASE)
        if m:
            rfc = m.group(1)
            j = i - 1
            while j >= 0 and not empresa:
                prev = lines[j].strip()
                if prev:
                    empresa = prev
                    break
                j -= 1
            break

    m2 = re.search(r"No\.\s*de\s*(?:cliente|cuenta)[: ]+(\d+)", full_text, re.IGNORECASE)
    if m2:
        no_cli = m2.group(1)

    return {"empresa": empresa, "rfc": rfc, "no_cliente": no_cli}

def _parse_accounts_summary(full_text: str) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    m = re.search(r"RESUMEN\s+INTEGRAL(.*?)(DETALLE\s+DE\s+MOVIMIENTOS|$)",
                  full_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return out
    block = m.group(1)

    def grab(label_pat: str, key: str):
        pat = re.compile(
            rf"({label_pat}).*?"
            r"(\d{6,})\s+"
            r"([\d ]{14,})\s+"
            r"\$?\s*([\d,]+\.\d{2})\s+"
            r"\$?\s*([\d,]+\.\d{2})",
            re.IGNORECASE | re.DOTALL
        )
        g = pat.search(block)
        if not g:
            return
        label_txt = g.group(1)
        cuenta = g.group(2)
        clabe = re.sub(r"\s+", "", g.group(3))
        saldo_ant = _norm_amount(g.group(4))
        saldo_corte = _norm_amount(g.group(5))
        seccion_hum = ("Enlace Negocios Avanzada"
                       if "AVANZADA" in label_txt.upper()
                       else "Enlace Negocios Basica" if "ENLACE" in label_txt.upper()
                       else label_txt.title())
        out[key] = {
            "label": seccion_hum,
            "cuenta": cuenta,
            "clabe": clabe,
            "saldo_anterior": saldo_ant,
            "saldo_corte": saldo_corte,
        }

    grab(r"ENLACE\s+NEGOCIOS\s+(?:BASICA|AVANZADA)", "BASICA")
    grab(r"INVERSION\s+ENLACE\s+NEGOCIOS", "INVERSION")
    return out

# -----------------------------------------------------------
# Secciones (Básica/Avanzada o Inversión) + limpieza
# -----------------------------------------------------------

def _make_label_pat(section_label) -> str:
    if isinstance(section_label, (list, tuple, set)):
        return "(?:" + "|".join(re.escape(x) for x in section_label) + ")"
    return re.escape(section_label)

def _slice_sections(full_text: str, section_label) -> str:
    """
    Recorta el bloque de 'DETALLE DE MOVIMIENTOS' y, además,
    corta cualquier texto “parásito” al encontrar los sentinelas.
    """
    t = full_text
    blocks: List[str] = []

    label_pat = _make_label_pat(section_label)

    pat_start = re.compile(
        r"DETALLE\s+DE\s+MOVIMIENTOS.*?" + label_pat,
        re.IGNORECASE | re.DOTALL
    )
    pat_end = re.compile(
        r"(ENLACE\s+NEGOCIOS\s+(?:BASICA|AVANZADA)|INVERSION\s+ENLACE\s+NEGOCIOS|"
        r"SALDO\s+PROMEDIO|RESUMEN\s+DEL\s+PERIODO|RESUMEN\s+INTEGRAL)",
        re.IGNORECASE
    )

    pos = 0
    while True:
        m = pat_start.search(t, pos)
        if not m:
            break
        start = m.end()
        m_end = pat_end.search(t, start)
        end = m_end.start() if m_end else len(t)
        raw_block = t[start:end]

        # Quita encabezado de tabla
        raw_block = re.sub(
            r"(?i)\bFECHA\s+DESCRIPCI[ÓO]N\s*/\s*ESTABLECIMIENTO\s+MONTO\s+DEL\s+DEP[ÓO]SITO\s+MONTO\s+DEL\s+RETIRO\s+SALDO\b",
            " ",
            raw_block
        )
        # Corta headers/pies comunes
        cut = _HEADER_OR_FOOTER_PAT.search(raw_block)
        if cut:
            raw_block = raw_block[:cut.start()]

        # Corte duro al encontrar sentinelas de texto parásito
        stop = _STOP_TRAIL_PAT.search(raw_block)
        if stop:
            raw_block = raw_block[:stop.start()]

        blocks.append(raw_block)
        pos = end

    if not blocks:
        pat_start2 = re.compile(label_pat, re.IGNORECASE)
        pos = 0
        while True:
            m = pat_start2.search(t, pos)
            if not m:
                break
            start = m.end()
            m_end = pat_end.search(t, start)
            end = m_end.start() if m_end else len(t)
            raw_block = t[start:end]
            cut = _HEADER_OR_FOOTER_PAT.search(raw_block)
            if cut:
                raw_block = raw_block[:cut.start()]
            stop = _STOP_TRAIL_PAT.search(raw_block)
            if stop:
                raw_block = raw_block[:stop.start()]
            blocks.append(raw_block)
            pos = end

    return "\n".join(blocks)

# -----------------------------------------------------------
# Conversión del bloque a filas
# -----------------------------------------------------------

def _parse_section_to_rows(block_text: str, period_start: Optional[date] = None) -> List[Dict]:
    if not block_text:
        return []

    # Seguridad extra: corta si aparecen sentinelas en el texto completo del bloque
    mstop = _STOP_TRAIL_PAT.search(block_text)
    txt = block_text[:mstop.start()] if mstop else block_text
    txt = txt.replace("\u00a0", " ")

    rows: List[Dict] = []

    # Saldo anterior (si está antes del primer movimiento)
    mfirst = _DATE_TOKEN.search(txt)
    if mfirst:
        prefix = txt[:mfirst.start()]
        if "SALDO ANTERIOR" in prefix.upper():
            am = _AMOUNT_RE.findall(prefix)
            saldo = _norm_amount(am[-1]) if am else 0.0
            dprev = (period_start - timedelta(days=1)) if period_start else date(1970, 1, 1)
            rows.append({
                "date": dprev, "description": "SALDO ANTERIOR",
                "withdrawal": 0.0, "deposit": 0.0, "balance": _round2(saldo)
            })

    matches = list(_DATE_TOKEN.finditer(txt))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(txt)
        chunk = txt[start:end].strip()

        fraw = m.group(1)
        try:
            fdate = _parse_fecha_es(fraw)
        except Exception:
            continue

        body = chunk[len(fraw):].strip(" \t:-\n\r")

        # En el último movimiento, corta si aparecen sentinelas
        if i == len(matches) - 1:
            sstop = _STOP_TRAIL_PAT.search(body)
            if sstop:
                body = body[:sstop.start()].rstrip()

        # --- Usar el BLOQUE (run) más a la derecha de 2 o 3 importes contiguos ---
        body_clean, tail = _extract_amount_run(body)
        dep, ret, bal = 0.0, 0.0, None

        if len(tail) == 3:
            dep, ret, bal = tail[0], tail[1], tail[2]
        elif len(tail) == 2:
            monto, bal = tail[0], tail[1]
            w, d = _classify(body_clean.upper(), monto)
            ret, dep = w, d
        elif len(tail) == 1:
            bal = tail[0]

        desc = re.sub(r"\s+", " ", body_clean).strip()

        rows.append({
            "date": fdate,
            "description": desc,
            "deposit": _round2(dep),
            "withdrawal": _round2(ret),
            "balance": _round2(bal),
        })

    return rows

# -----------------------------------------------------------
# Periodo
# -----------------------------------------------------------

def _parse_period(full_text: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(
        r"Periodo\s+Del\s+(\d{2}[/\- ][A-Za-zÁá]{3}[/\- ]\d{2,4})\s+al\s+(\d{2}[/\- ][A-Za-zÁá]{3}[/\- ]\d{2,4})",
        full_text, re.IGNORECASE
    )
    if not m:
        return None, None

    def _p(s: str) -> date:
        s0 = s.strip()
        d, mon, y = re.match(r"(\d{2})[/\- ]([A-Za-zÁá]{3})[/\- ](\d{2,4})", s0).groups()
        yy = int(y) if len(y) == 4 else 2000 + int(y)
        mm = MES_ABREV[mon.lower().replace("á", "a")]
        return date(yy, mm, int(d))

    return _p(m.group(1)), _p(m.group(2))

# -----------------------------------------------------------
# Punto de entrada
# -----------------------------------------------------------

def extract_banorte_to_xlsx(pdf_path: str, out_xlsx: str) -> None:
    with pdfplumber.open(pdf_path) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    full = "\n".join(pages).replace("\u00a0", " ")

    period_start, period_end = _parse_period(full)

    header = _parse_header_info(full)
    resumen = _parse_accounts_summary(full)

    # Acepta Básica o Avanzada
    basica_block = _slice_sections(full, ["ENLACE NEGOCIOS BASICA", "ENLACE NEGOCIOS AVANZADA"])
    inv_block    = _slice_sections(full, "INVERSION ENLACE NEGOCIOS")

    basica_rows = _parse_section_to_rows(basica_block, period_start=period_start)
    inv_rows    = _parse_section_to_rows(inv_block,    period_start=period_start)

    cols_in = ["date", "description", "deposit", "withdrawal", "balance"]
    def _df(rows: List[Dict]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=cols_in)
        for r in rows:
            r["date"] = r["date"].strftime("%d-%m-%Y") if isinstance(r["date"], date) else r["date"]
        return pd.DataFrame([{
            "date": r["date"],
            "description": r["description"],
            "deposit": r["deposit"],
            "withdrawal": r["withdrawal"],
            "balance": r["balance"],
        } for r in rows], columns=cols_in)

    df1 = _df(basica_rows)
    df2 = _df(inv_rows)

    rename = {
        "date": "Fecha",
        "description": "Descripción",
        "deposit": "Depósitos/Abonos",
        "withdrawal": "Retiros/Cargos",
        "balance": "Saldo",
    }
    df1.rename(columns=rename, inplace=True)
    df2.rename(columns=rename, inplace=True)

    info = pd.DataFrame([{
        "Banco": "Banorte",
        "Archivo": Path(pdf_path).name,
        "Periodo inicio": period_start.strftime("%d-%m-%Y") if period_start else None,
        "Periodo fin": period_end.strftime("%d-%m-%Y") if period_end else None,
        "Empresa": header.get("empresa"),
        "RFC": header.get("rfc"),
        "No. de cliente / cuenta": header.get("no_cliente"),
    }])

    cuentas_rows = []
    if "BASICA" in resumen:
        cuentas_rows.append({
            "Sección": resumen["BASICA"].get("label", "Enlace Negocios Basica"),
            "Cuenta": resumen["BASICA"].get("cuenta"),
            "Clabe": resumen["BASICA"].get("clabe"),
            "Saldo Anterior": resumen["BASICA"].get("saldo_anterior"),
        })
    if "INVERSION" in resumen:
        cuentas_rows.append({
            "Sección": "Inversión Enlace Negocios",
            "Cuenta": resumen["INVERSION"].get("cuenta"),
            "Clabe": resumen["INVERSION"].get("clabe"),
            "Saldo Anterior": resumen["INVERSION"].get("saldo_anterior"),
        })
    cuentas_df = pd.DataFrame(cuentas_rows, columns=["Sección","Cuenta","Clabe","Saldo Anterior"])

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        info.to_excel(xl, index=False, sheet_name="info")
        cuentas_df.to_excel(xl, index=False, sheet_name="cuentas")
        df1.to_excel(xl, index=False, sheet_name="cuenta_01")
        df2.to_excel(xl, index=False, sheet_name="cuenta_02")
