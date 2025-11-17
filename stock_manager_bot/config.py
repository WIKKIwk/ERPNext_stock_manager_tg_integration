from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, MutableMapping, Optional

DEFAULT_DB_PATH = Path("stock_manager_bot.sqlite3")


@dataclass(frozen=True)
class StockBotConfig:
    token: str
    db_path: Path
    frappe_base_url: str
    verify_endpoint: str
    item_limit: int
    entry_series_template: str
    purchase_receipt_series_template: str
    delivery_note_series_template: str
    default_company: str
    warehouse_limit: int
    supplier_limit: int
    purchase_receipt_limit: int
    customer_limit: int
    delivery_note_limit: int


def _parse_limit(raw: Optional[str], fallback: int) -> int:
    if not raw:
        return fallback
    try:
        return max(1, int(raw))
    except ValueError:
        return fallback


def load_config(env: Optional[Mapping[str, str]] = None) -> StockBotConfig:
    source = env or os.environ

    token = source.get("STOCK_BOT_TOKEN") or source.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("STOCK_BOT_TOKEN (yoki TELEGRAM_BOT_TOKEN) talab qilinadi.")

    base_url = source.get("FRAPPE_BASE_URL")
    if not base_url:
        raise RuntimeError("FRAPPE_BASE_URL sozlanishi kerak.")
    base_url = base_url.strip().rstrip("/")
    if not base_url:
        raise RuntimeError("FRAPPE_BASE_URL noto'g'ri.")

    verify_endpoint = (
        source.get("ERP_VERIFY_ENDPOINT") or "/api/method/frappe.auth.get_logged_user"
    ).strip()
    if not verify_endpoint.startswith("/"):
        verify_endpoint = "/" + verify_endpoint

    db_value = source.get("STOCK_BOT_DB_PATH")
    db_path = Path(db_value).expanduser() if db_value else DEFAULT_DB_PATH

    item_limit = _parse_limit(source.get("ITEM_LIMIT"), 25)
    warehouse_limit = _parse_limit(source.get("WAREHOUSE_LIMIT"), 25)
    supplier_limit = _parse_limit(source.get("SUPPLIER_LIMIT"), 25)
    purchase_receipt_limit = _parse_limit(source.get("PURCHASE_RECEIPT_LIMIT"), 25)
    customer_limit = _parse_limit(source.get("CUSTOMER_LIMIT"), 25)
    delivery_note_limit = _parse_limit(source.get("DELIVERY_NOTE_LIMIT"), 25)

    entry_series = source.get("STOCK_ENTRY_SERIES") or "MAT-STE-.YYYY.-.#####"
    entry_series = entry_series.strip() or "MAT-STE-.YYYY.-.#####"
    purchase_receipt_series = source.get("PURCHASE_RECEIPT_SERIES") or "MAT-PRE-.YYYY.-.#####"
    purchase_receipt_series = purchase_receipt_series.strip() or "MAT-PRE-.YYYY.-.#####"
    delivery_note_series = source.get("DELIVERY_NOTE_SERIES") or "MAT-DN-.YYYY.-.#####"
    delivery_note_series = delivery_note_series.strip() or "MAT-DN-.YYYY.-.#####"
    company = source.get("ERP_COMPANY") or source.get("DEFAULT_COMPANY") or "accord"

    return StockBotConfig(
        token=token.strip(),
        db_path=db_path,
        frappe_base_url=base_url,
        verify_endpoint=verify_endpoint,
        item_limit=item_limit,
        entry_series_template=entry_series,
        purchase_receipt_series_template=purchase_receipt_series,
        delivery_note_series_template=delivery_note_series,
        default_company=company.strip(),
        warehouse_limit=warehouse_limit,
        supplier_limit=supplier_limit,
        purchase_receipt_limit=purchase_receipt_limit,
        customer_limit=customer_limit,
        delivery_note_limit=delivery_note_limit,
    )


def override_env_for_tests(
    env: MutableMapping[str, str],
    *,
    token: str = "TEST_TOKEN",
    base_url: str = "https://example.com",
    db_path: Optional[str] = None,
) -> None:
    env["STOCK_BOT_TOKEN"] = token
    env["FRAPPE_BASE_URL"] = base_url
    if db_path is not None:
        env["STOCK_BOT_DB_PATH"] = db_path
