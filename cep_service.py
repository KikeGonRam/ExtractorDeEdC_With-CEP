# cep_service.py
from __future__ import annotations
"""
Puente del backend a la librería de CEPs.
Genera el ZIP (Excel + PDFs) a partir de un PDF y el extractor indicado.
"""

import os
from pathlib import Path
from typing import Optional

from cep_banxico import build_zip_with_ceps_from_pdf  # toda la lógica vive aquí
from inbursa_extractor import extract_inbursa_to_xlsx  # NEW


def make_zip_with_ceps_for_bank(
    bank: str,
    input_pdf: str,
    workdir: str,
    headless: Optional[bool] = None,
) -> str:
    bank = (bank or "").strip().lower()
    if bank not in ("santander", "bbva", "banorte", "inbursa"):
        raise ValueError("Banco no soportado. Usa: santander | bbva | banorte | inbursa")

    if headless is None:
        headless = bool(int(os.environ.get("CEP_HEADLESS", "1")))

    workdir_path = Path(workdir)
    workdir_path.mkdir(parents=True, exist_ok=True)
    pdf_path = Path(input_pdf)

    zip_out = workdir_path / f"{pdf_path.stem}_{bank}_with_ceps.zip"

    res = build_zip_with_ceps_from_pdf(
        pdf_path=str(pdf_path),
        extractor=bank,
        zip_out=str(zip_out),
        headless=headless,
    )
    return str(Path(res["zip"]).resolve())
