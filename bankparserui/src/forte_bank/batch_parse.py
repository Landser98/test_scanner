# src/forte_bank/batch_parse.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch / single-file parser for ForteBank statements + IP income.

Pipeline per PDF:
  1) ensure *_pages.jsonl (pdfplumber pages dump) exists
  2) ensure <stem>.json (PDF metadata + pages structure) exists
  3) parse header / tx / footer via parse_forte_statement
  4) numeric checks via generic statement_validation + FORTE schema
  5) PDF metadata validation (creation/mod dates, creator/producer, etc.)
  6) extract KNP, compute IP income
  7) save CSVs:
       <stem>_header.csv
       <stem>_tx.csv
       <stem>_footer.csv
       <stem>_meta.csv
       <stem>_tx_ip.csv
       <stem>_ip_income_monthly.csv
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import pikepdf

from src.utils import warnings_setup  # noqa: F401  (side-effect: suppress warnings)

from src.forte_bank.parser import parse_forte_statement, _extract_knp_from_purpose
from src.utils.income_calc import compute_ip_income
from src.utils.statement_validation import (
    BANK_SCHEMAS,
    validate_statement_generic,
    validate_pdf_metadata_from_json,
)
from src.utils.convert_pdf_json_pages import dump_pdf_pages
from src.utils.convert_pdf_json_page import dump_catalog, dump_pages


# ---------------------------------------------------------------------------
# Helpers: JSONL pages + PDF meta JSON
# ---------------------------------------------------------------------------

def ensure_jsonl_for_pdf(
    pdf_path: Path,
    jsonl_dir: Path,
    suffix: str = "_pages.jsonl",
) -> Path:
    """
    Ensure we have pdfplumber-style pages JSONL for this PDF.

    If missing, create it via dump_pdf_pages().
    """
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    out_path = jsonl_dir / f"{pdf_path.stem}{suffix}"

    if out_path.exists():
        return out_path

    print(f"[jsonl] Creating {out_path.name} from {pdf_path.name}")
    dump_pdf_pages(
        pdf_path=pdf_path,
        out_path=out_path,
        stream_preview_len=4000,
        include_full_stream=False,
    )
    return out_path


def ensure_pdf_meta_json(pdf_path: Path, meta_dir: Path) -> Path:
    """
    Ensure we have a single JSON with PDF metadata + pages structure
    for pdf_path in meta_dir.

    Uses pikepdf + dump_catalog + dump_pages from convert_pdf_json_page.py.
    """
    from src.utils.path_security import open_validated_path, sanitize_filename, validate_path_for_write
    meta_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = sanitize_filename(pdf_path.stem)
    json_path = meta_dir / f"{safe_stem}.json"

    if json_path.exists():
        return json_path

    print(f"[meta-json] Creating {json_path.name} from {pdf_path.name}")

    with pikepdf.open(str(pdf_path)) as pdf:
        out: Dict[str, Any] = {
            "file": str(pdf_path),
            "num_pages": len(pdf.pages),
        }
        out.update(
            dump_catalog(
                pdf,
                max_depth=6,
                include_streams=False,
                stream_max_bytes=0,
            )
        )
        out.update(
            dump_pages(
                pdf,
                max_depth=6,
                include_streams=False,
                stream_max_bytes=0,
            )
        )

    validated = validate_path_for_write(json_path, meta_dir)
    with open_validated_path(validated, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)

    return json_path



# ---------------------------------------------------------------------------
# Per-PDF processing
# ---------------------------------------------------------------------------

def process_one_forte(
    pdf_path: Path,
    jsonl_dir: Path,
    out_dir: Path,
    pdf_meta_dir: Path,
    jsonl_suffix: str = "_pages.jsonl",
    months_back: int | None = 12,
    verbose: bool = True,
) -> None:
    print(f"\n=== Processing Forte: {pdf_path.name} ===")

    # 1) ensure pages JSONL
    jsonl_path = ensure_jsonl_for_pdf(pdf_path, jsonl_dir, suffix=jsonl_suffix)

    # 2) parse header / tx / footer
    header_df, tx_df, footer_df = parse_forte_statement(
        str(pdf_path),
        str(jsonl_path),
    )

    # basic sanity for tx columns
    required_cols = [
        "Күні/Дата",
        "Кредит",
        "Назначение платежа",
        "Жіберуші/Отправитель",
    ]
    missing = [c for c in required_cols if c not in tx_df.columns]
    if missing:
        raise ValueError(f"Missing columns in tx_df for {pdf_path.name}: {missing}")
    header_df = header_df.copy()

    if "closing_balance" not in header_df.columns and "closing_balance" in footer_df.columns:
        header_df["closing_balance"] = footer_df.iloc[0]["closing_balance"]

    if "total_credit" not in header_df.columns and "total_credit" in footer_df.columns:
        header_df["total_credit"] = footer_df.iloc[0]["total_credit"]

    if "total_debit" not in header_df.columns and "total_debit" in footer_df.columns:
        header_df["total_debit"] = footer_df.iloc[0]["total_debit"]

    # 3) numeric validation via generic schema (if configured)
    num_flags: List[str] = []
    num_debug: Dict[str, Any] = {}

    schema = BANK_SCHEMAS.get("FORTE")
    if schema is not None:
        num_flags, num_debug = validate_statement_generic(
            header_df,
            tx_df,
            footer_df,
            schema,
        )
    else:
        num_debug = {
            "note": "No 'FORTE' schema in BANK_SCHEMAS; numeric validation skipped."
        }

    # 4) PDF metadata validation
    pdf_flags: List[str] = []
    pdf_debug: Dict[str, Any] = {}

    meta_json_path = ensure_pdf_meta_json(pdf_path, pdf_meta_dir)
    try:
        from src.utils.path_security import open_validated_path, validate_path
        validated = validate_path(meta_json_path, pdf_meta_dir)
        with open_validated_path(validated, "r", encoding="utf-8") as f:
            pdf_json = json.load(f)

        period_end = header_df.iloc[0].get("period_end")

        pdf_flags, pdf_debug = validate_pdf_metadata_from_json(
            pdf_json,
            bank="FORTE",
            period_end=period_end,
            period_date_format="%d.%m.%Y",
            max_days_after_period_end=7,
            allowed_creators=None,   # tighten once you know real values
            allowed_producers=None,
        )
    except Exception as e:
        pdf_flags = ["pdf_meta_validation_error"]
        pdf_debug = {"error": str(e), "meta_json_path": str(meta_json_path)}

    # 5) КНП (if missing)
    if "КНП" not in tx_df.columns:
        tx_df["КНП"] = (
            tx_df["Назначение платежа"]
            .fillna("")
            .astype(str)
            .apply(_extract_knp_from_purpose)
        )

    # 6) IP income
    enriched_tx, monthly_income, income_summary = compute_ip_income(
        tx_df,
        col_op_date="Күні/Дата",
        col_credit="Кредит",
        col_knp="КНП",
        col_purpose="Назначение платежа",
        col_counterparty="Жіберуші/Отправитель",
        months_back=months_back,
        verbose=verbose,
        max_examples=5,
    )

    # 7) meta_df (numeric + pdf flags + debug_info)
    all_flags = num_flags + pdf_flags
    all_debug = {
        "numeric": num_debug,
        "pdf_meta": pdf_debug,
        "jsonl_file": str(jsonl_path),
    }

    meta_df = pd.DataFrame(
        [{
            "bank": "FORTE",
            "pdf_file": pdf_path.name,
            "jsonl_file": jsonl_path.name,
            "flags": ";".join(all_flags),
            "debug_info": json.dumps(all_debug, ensure_ascii=False),
        }]
    )

    # 8) paths
    stem = pdf_path.stem
    header_path   = out_dir / f"{stem}_header.csv"
    tx_path       = out_dir / f"{stem}_tx.csv"
    footer_path   = out_dir / f"{stem}_footer.csv"
    meta_path     = out_dir / f"{stem}_meta.csv"
    enriched_path = out_dir / f"{stem}_tx_ip.csv"
    monthly_path  = out_dir / f"{stem}_ip_income_monthly.csv"
    income_summary_path = out_dir / f"{stem}_income_summary.csv"
    income_summary_df = pd.DataFrame([income_summary])

    # 9) save CSVs
    out_dir.mkdir(parents=True, exist_ok=True)

    header_df.to_csv(header_path, index=False, encoding="utf-8-sig")
    tx_df.to_csv(tx_path, index=False, encoding="utf-8-sig")
    footer_df.to_csv(footer_path, index=False, encoding="utf-8-sig")
    meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
    enriched_tx.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    monthly_income.to_csv(monthly_path, index=False, encoding="utf-8-sig")
    income_summary_df.to_csv(income_summary_path, index=False, encoding="utf-8-sig")

    print(f"  → Header:      {header_df.shape[0]} row   → {header_path}")
    print(f"  → Tx:          {tx_df.shape[0]} rows → {tx_path}")
    print(f"  → Footer:      {footer_df.shape[0]} row   → {footer_path}")
    print(f"  → Meta:        {meta_df.shape[0]} row   → {meta_path}")
    print(f"  → Tx+IP flags: {enriched_tx.shape[0]} rows → {enriched_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"  → IP monthly:  {monthly_income.shape[0]} rows → {monthly_path}")
    print(f"  → Income summary: {income_summary_path}")
    print(f"✅ Adjusted income: {income_summary['total_income_adjusted']:,.2f} KZT")



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Parse ForteBank statements (single file or folder), "
            "auto-create pages JSONL and PDF meta JSON, validate, and compute IP income."
        )
    )
    ap.add_argument(
        "path",
        help="PDF file or directory with PDFs (e.g. data/forte)",
    )
    ap.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern when path is a directory (default: '*.pdf')",
    )
    ap.add_argument(
        "--jsonl-dir",
        help=(
            "Directory to store/load *_pages.jsonl "
            "(default: <path>/converted_jsons or <file_dir>/converted_jsons)"
        ),
    )
    ap.add_argument(
        "--pdf-meta-dir",
        help=(
            "Directory to store/load PDF metadata JSONs "
            "(default: <path>/pdf_meta or <file_dir>/pdf_meta)"
        ),
    )
    ap.add_argument(
        "--out-dir",
        help="Output directory for CSVs (default: <path>/out or <file_dir>/out)",
    )
    ap.add_argument(
        "--jsonl-suffix",
        default="_pages.jsonl",
        help="Suffix for JSONL filenames (default: '_pages.jsonl')",
    )
    ap.add_argument(
        "--months-back",
        type=int,
        default=12,
        help="How many last months to consider for IP income (default: 12)",
    )
    ap.add_argument(
        "--no-verbose",
        action="store_true",
        help="Disable detailed income_calc logging",
    )

    args = ap.parse_args()

    in_path = Path(args.path)

    if in_path.is_file():
        pdf_files = [in_path]
        base_dir = in_path.parent
        base_name = in_path.stem
    else:
        if not in_path.is_dir():
            raise SystemExit(f"Path not found or not a directory: {in_path}")
        pdf_files = sorted(in_path.rglob(args.pattern))
        base_dir = in_path
        base_name = in_path.name

    if not pdf_files:
        raise SystemExit(f"No PDF files found under {in_path} with pattern {args.pattern}")

    default_out_dir = base_dir.parent / f"{base_name}_out"
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else base_dir / "converted_jsons"
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    pdf_meta_dir = Path(args.pdf_meta_dir) if args.pdf_meta_dir else base_dir / "pdf_meta"
    pdf_meta_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} Forte statement(s).")
    print(f"CSV output dir:   {out_dir}")
    print(f"Pages JSONL dir:  {jsonl_dir}")
    print(f"PDF meta dir:     {pdf_meta_dir}")

    for pdf in pdf_files:
        try:
            process_one_forte(
                pdf_path=pdf,
                jsonl_dir=jsonl_dir,
                out_dir=out_dir,
                pdf_meta_dir=pdf_meta_dir,
                jsonl_suffix=args.jsonl_suffix,
                months_back=args.months_back,
                verbose=not args.no_verbose,
            )
        except Exception as e:
            print(f"❌ Failed to process {pdf.name}: {e}")


if __name__ == "__main__":
    main()
