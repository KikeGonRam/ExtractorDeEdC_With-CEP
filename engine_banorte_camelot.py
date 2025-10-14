# engine_banorte_camelot.py
import pdfplumber
from openpyxl import Workbook

def extract_banorte_to_xlsx(pdf_path: str, out_xlsx: str) -> None:
    """
    Mínimo funcional para probar el pipeline.
    Lee algo de texto del PDF y genera un XLSX con dos hojas.
    Sustituye el contenido por tu lógica real cuando lo desees.
    """
    # Leer primeras líneas del PDF (solo para demo)
    first_lines = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if pdf.pages:
                txt = (pdf.pages[0].extract_text() or "").replace("\u00a0", " ")
                first_lines = [ln for ln in txt.splitlines() if ln.strip()][:15]
    except Exception:
        first_lines = ["(No pude leer texto del PDF)"]

    # Crear Excel de ejemplo
    wb = Workbook()
    ws_info = wb.active
    ws_info.title = "info"
    ws_info.append(["campo", "valor"])
    ws_info.append(["origen_pdf", pdf_path])
    for i, ln in enumerate(first_lines, start=1):
        ws_info.append([f"linea_{i}", ln])

    ws_movs = wb.create_sheet("movs")
    ws_movs.append(["date", "description", "deposits", "withdrawals", "balance"])
    # Rellena filas de ejemplo (para verificar columnas)
    ws_movs.append(["2024-08-01", "DEMO: Movimiento 1", 0, 100.50, 9900.00])
    ws_movs.append(["2024-08-02", "DEMO: Movimiento 2", 2500.00, 0, 12400.00])

    wb.save(out_xlsx)
