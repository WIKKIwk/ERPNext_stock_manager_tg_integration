from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultsButton,
    InputTextMessageContent,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType
from telegram.ext import (
    AIORateLimiter,
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

from .config import StockBotConfig, load_config
from .purchase import (
    PurchaseFlowMixin,
    PURCHASE_TRIGGER,
    PURCHASE_CALLBACK_CREATE,
    PURCHASE_CREATE_PREFIX,
    PURCHASE_SUPPLIER_QUERY_PREFIX,
    PURCHASE_ITEM_QUERY_PREFIX,
    PURCHASE_APPROVE_PREFIX,
    PURCHASE_CANCEL_PREFIX,
    PURCHASE_DELETE_PREFIX,
    PURCHASE_CONFIRM_CALLBACK,
    PURCHASE_APPROVE_QUERY_PREFIXES,
)
from .delivery import (
    DeliveryFlowMixin,
    DELIVERY_TRIGGER,
    DELIVERY_CALLBACK_CREATE,
    DELIVERY_CREATE_PREFIX,
    DELIVERY_CUSTOMER_QUERY_PREFIX,
    DELIVERY_ITEM_QUERY_PREFIX,
    DELIVERY_APPROVE_PREFIX,
    DELIVERY_CANCEL_PREFIX,
    DELIVERY_DELETE_PREFIX,
    DELIVERY_CONFIRM_CALLBACK,
    DELIVERY_APPROVE_QUERY_PREFIXES,
)
from .storage import StockStorage

logger = logging.getLogger(__name__)

ENTRY_TRIGGER = "entry"
ENTRY_CALLBACK_CREATE = "entry:create"
ENTRY_APPROVE_PREFIX = "entry-approve"
ENTRY_CANCEL_PREFIX = "entry-cancel"
ENTRY_DELETE_PREFIX = "entry-delete"
ENTRY_CREATE_PREFIX = "entrycreate"
ENTRY_APPROVE_QUERY_PREFIXES = ("entryapprove", "approve")
ENTRY_TYPE_OPTIONS = {
    "receipt": {
        "label": "Material kiridi",
        "value": "Material Receipt",
        "warehouse_role": "target",
    },
    "issue": {
        "label": "Material chiqdi",
        "value": "Material Issue",
        "warehouse_role": "source",
    },
}


class StockManagerBot(DeliveryFlowMixin, PurchaseFlowMixin):
    """Telegram bot that verifies ERPNext API keys and lists Item records."""

    def __init__(self, config: StockBotConfig, storage: Optional[StockStorage] = None):
        self.config = config
        self.storage = storage or StockStorage(config.db_path)
        self.application = (
            Application.builder()
            .token(config.token)
            .rate_limiter(AIORateLimiter())
            .post_init(self._post_init)
            .build()
        )
        self._register_handlers()
        self._bot_username: Optional[str] = None

    async def _post_init(self, application: Application) -> None:
        me = await application.bot.get_me()
        self._bot_username = me.username
        logger.info("Stock manager bot connected as %s (@%s)", me.full_name, me.username)

    def _register_handlers(self) -> None:
        app = self.application
        app.add_handler(CommandHandler("start", self.handle_start))
        app.add_handler(CommandHandler("help", self.handle_help))
        app.add_handler(CommandHandler("items", self.handle_items))
        app.add_handler(CommandHandler("entry", self.handle_entry_command))
        app.add_handler(CommandHandler("purchase", self.handle_purchase_command))
        app.add_handler(CommandHandler("delivery", self.handle_delivery_command))
        app.add_handler(CommandHandler("clear", self.handle_clear_command))
        app.add_handler(CommandHandler("cancel", self.handle_cancel_command))
        app.add_handler(
            MessageHandler(
                filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND),
                self.handle_private_message,
            )
        )
        app.add_handler(InlineQueryHandler(self.handle_inline_query))
        app.add_handler(CallbackQueryHandler(self.handle_item_callback, pattern=r"^item:"))
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_detail_callback, pattern=r"^entry-detail:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_create_callback, pattern=r"^entry:create$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_creation_callback, pattern=r"^entrycreate:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_create_callback, pattern=r"^purchase:create$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_creation_callback, pattern=r"^purchasecreate:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_confirm_callback, pattern=rf"^{PURCHASE_CONFIRM_CALLBACK}$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_approve_action, pattern=rf"^{PURCHASE_APPROVE_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_cancel_action, pattern=rf"^{PURCHASE_CANCEL_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_purchase_delete_action, pattern=rf"^{PURCHASE_DELETE_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_create_callback, pattern=r"^delivery:create$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_creation_callback, pattern=r"^deliverycreate:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_confirm_callback, pattern=rf"^{DELIVERY_CONFIRM_CALLBACK}$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_approve_action, pattern=rf"^{DELIVERY_APPROVE_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_cancel_action, pattern=rf"^{DELIVERY_CANCEL_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_delivery_delete_action, pattern=rf"^{DELIVERY_DELETE_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_confirm_callback, pattern=r"^entry:confirm$")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_approve_callback, pattern=rf"^{ENTRY_APPROVE_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_cancel_callback, pattern=rf"^{ENTRY_CANCEL_PREFIX}:")
        )
        app.add_handler(
            CallbackQueryHandler(self.handle_entry_delete_callback, pattern=rf"^{ENTRY_DELETE_PREFIX}:")
        )
        app.add_error_handler(self.handle_error)

    # ------------------------------------------------------ validation helpers
    @staticmethod
    def _validate_token(value: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z0-9]{14,18}", value))

    @staticmethod
    def _safe_text_preview(value: str, limit: int = 80) -> str:
        trimmed = (value or "").strip()
        if not trimmed:
            return ""
        if re.fullmatch(r"[A-Za-z0-9]{10,}$", trimmed):
            return "<token>"
        single_line = re.sub(r"\s+", " ", trimmed)
        if len(single_line) <= limit:
            return single_line
        return single_line[: limit - 1] + "…"

    def _log_event(self, user_id: Optional[int], action: str, **extra: Any) -> None:
        details = " ".join(f"{key}={value}" for key, value in extra.items() if value not in (None, ""))
        logger.info("event=%s user=%s %s", action, user_id or "-", details.strip())

    def _items_markup(self) -> InlineKeyboardMarkup:
        label = "📦 Itemlarni ko'rish"
        button = InlineKeyboardButton(label, switch_inline_query_current_chat="items")
        return InlineKeyboardMarkup([[button]])

    def _main_menu_markup(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            [
                ["📦 Itemlar", "📋 Harakatlar"],
                ["🧾 Purchase Receipt", "🚚 Delivery Note"],
                ["♻️ API ni tozalash"],
            ],
            resize_keyboard=True,
            one_time_keyboard=False,
        )

    @staticmethod
    def _inline_start_button(text: str) -> InlineQueryResultsButton:
        label = (text or "Botni ochish").strip() or "Botni ochish"
        return InlineQueryResultsButton(text=label[:48], start_parameter="start")

    def _entry_markup(self) -> InlineKeyboardMarkup:
        view_button = InlineKeyboardButton(
            "📋 Harakatni ko'rish", switch_inline_query_current_chat=ENTRY_TRIGGER
        )
        create_button = InlineKeyboardButton(
            "➕ Yangi harakat yaratish", callback_data=ENTRY_CALLBACK_CREATE
        )
        confirm_button = InlineKeyboardButton(
            "✔️ Harakatni tasdiqlash", callback_data="entry:confirm"
        )
        return InlineKeyboardMarkup([[view_button], [create_button], [confirm_button]])

    def _cancel_creation_button(self, prefix: str = ENTRY_CREATE_PREFIX) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            "❌ Jarayonni bekor qilish", callback_data=f"{prefix}:cancel"
        )

    def _cancel_creation_markup(self, prefix: str = ENTRY_CREATE_PREFIX) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[self._cancel_creation_button(prefix=prefix)]])

    @staticmethod
    def _clean_text(value: Optional[str]) -> str:
        if not value:
            return ""
        return re.sub(r"<[^>]+>", " ", value).strip()

    @staticmethod
    def _docstatus_label(value: Optional[int]) -> str:
        mapping = {0: "Draft", 1: "Tasdiqlangan", 2: "Bekor qilingan"}
        return mapping.get(value, "Noma'lum")

    @staticmethod
    def _parse_warehouse_inline(text: str) -> Optional[Dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return None
        if not any("warehouse" in line.lower() for line in lines):
            return None
        code = None
        label = None
        for line in lines:
            lowered = line.lower()
            if lowered.startswith("warehouse:"):
                label = line.split(":", 1)[1].strip()
            if lowered.startswith("entry warehouse:"):
                label = line.split(":", 1)[1].strip()
            if lowered.startswith("code:"):
                code = line.split(":", 1)[1].strip()
        if not code:
            code = label
        if not code:
            return None
        return {"code": code, "label": label or code}


    @staticmethod
    def _build_item_keyboard(rows: list[Dict[str, Any]]) -> InlineKeyboardMarkup:
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows[:10]:
            docname = row.get("item_code") or row.get("name")
            if not docname:
                continue
            label = row.get("item_name") or docname
            buttons.append([InlineKeyboardButton(label[:60], callback_data=f"item:{docname}")])
        if not buttons:
            buttons = [[InlineKeyboardButton("Yangilash", callback_data="item:refresh")]]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def _build_entry_keyboard(rows: list[Dict[str, Any]]) -> InlineKeyboardMarkup:
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows[:10]:
            docname = row.get("name")
            if not docname:
                continue
            purpose = row.get("purpose") or row.get("stock_entry_type") or "-"
            label = f"{docname} ({purpose})"
            buttons.append([InlineKeyboardButton(label[:60], callback_data=f"entry-detail:{docname}")])
        if not buttons:
            buttons = [[InlineKeyboardButton("Yangilash", callback_data="entry-detail:refresh")]]
        return InlineKeyboardMarkup(buttons)

    async def _send_item_preview(
        self,
        *,
        chat_id: int,
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        success, error_detail, rows = await self._fetch_items(api_key, api_secret, query="")
        if not success:
            text = "Item ro'yxatini olishda xatolik yuz berdi."
            if error_detail:
                text += f"\nMa'lumot: {error_detail}"
            await context.bot.send_message(chat_id=chat_id, text=text)
            return
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="ERPNext da item topilmadi.")
            return
        button = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📦 Item oynasini ochish",
                        switch_inline_query_current_chat="itemlookup",
                    )
                ]
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Item tanlash uchun pastdagi qidiruv oynasini oching.",
            reply_markup=button,
        )

    async def _send_entry_preview(
        self,
        *,
        chat_id: int,
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
        show_message: bool = True,
    ) -> None:
        success, error_detail, rows = await self._fetch_stock_entries(
            api_key,
            api_secret,
            query="",
        )
        if not success:
            if show_message:
                text = "Stock Entry ro'yxatini olishda xatolik yuz berdi."
                if error_detail:
                    text += f"\nMa'lumot: {error_detail}"
                await context.bot.send_message(chat_id=chat_id, text=text)
            return
        if not rows:
            if show_message:
                await context.bot.send_message(chat_id=chat_id, text="Hozircha Stock Entry topilmadi.")
            return
        if not show_message:
            return
        preview = rows[:5]
        lines = []
        for row in preview:
            name = row.get("name") or "-"
            purpose = row.get("purpose") or row.get("stock_entry_type") or "-"
            posting = row.get("posting_date") or "-"
            warehouses = f"{row.get('from_warehouse') or '-'} → {row.get('to_warehouse') or '-'}"
            status = self._docstatus_label(row.get("docstatus"))
            lines.append(f"• {name} — {purpose} ({posting}, {warehouses}) — {status}")
        if len(rows) > len(preview):
            lines.append(f"... yana {len(rows) - len(preview)} ta harakat inline menyuda mavjud.")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))

    async def _start_entry_creation(
        self,
        *,
        user_id: int,
        chat_id: int,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        draft = {
            "kind": "stock_entry",
            "stage": "await_type",
            "chat_id": chat_id,
            "series": self.config.entry_series_template,
        }
        self.storage.save_entry_draft(user_id, draft)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📄 Yangi Stock Entry seriyasi: {draft['series']}\n"
                "Iltimos, harakat turini tanlang."
                "\nJarayonni to'xtatish uchun 'Bekor qilish' tugmasidan foydalanishingiz mumkin."
            ),
        )
        await self._prompt_entry_type(chat_id=chat_id, context=context)

    async def _cancel_entry_creation(
        self,
        *,
        user_id: int,
        chat_id: int,
        context: ContextTypes.DEFAULT_TYPE,
        notice: Optional[str] = None,
    ) -> None:
        self.storage.delete_entry_draft(user_id)
        message = notice or "Yangi Stock Entry jarayoni bekor qilindi."
        await context.bot.send_message(chat_id=chat_id, text=message)

    async def _prompt_entry_type(self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        buttons = [
            [
                InlineKeyboardButton(
                    opt["label"],
                    callback_data=f"{ENTRY_CREATE_PREFIX}:type:{key}",
                )
            ]
            for key, opt in ENTRY_TYPE_OPTIONS.items()
        ]
        buttons.append([self._cancel_creation_button()])
        await context.bot.send_message(
            chat_id=chat_id,
            text="Harakat turini tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )


    async def _prompt_entry_item(
        self,
        *,
        user_id: int,
        chat_id: int,
        api_key: str,
        api_secret: str,
        draft: Dict[str, Any],
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        draft["stage"] = "await_item_message"
        self.storage.save_entry_draft(user_id, draft)
        button = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📦 Item oynasini ochish", switch_inline_query_current_chat="entryitem "
                    )
                ],
                [self._cancel_creation_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Qaysi item keldi/ketyapti?\n"
                "Inline menyudan tanlagach xabar shu chatda paydo bo'ladi."
            ),
            reply_markup=button,
        )

    async def _prompt_entry_warehouse(
        self,
        *,
        user_id: int,
        chat_id: int,
        api_key: str,
        api_secret: str,
        draft: Dict[str, Any],
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        draft["stage"] = "await_warehouse_message"
        self.storage.save_entry_draft(user_id, draft)
        role = draft.get("warehouse_role") or "target"
        prompt = "Qaysi omborga kelgan?" if role == "target" else "Qaysi ombordan chiqyapti?"
        button = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🏬 Ombor oynasini ochish",
                        switch_inline_query_current_chat="entrywarehouse ",
                    )
                ],
                [self._cancel_creation_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=prompt,
            reply_markup=button,
        )

    async def _handle_entry_item_message(
        self,
        *,
        user_id: int,
        message,
        text: str,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            await message.reply_text(
                "Inline menyudan item tanlab shu chatga yuboring. Xabar tarkibida \"Item Code:\" bo'lishi kerak."
            )
            return True
        if not any("item code" in line.lower() or "#entryitem" in line.lower() for line in lines):
            return False
        code = None
        name = None
        uom = None
        for line in lines:
            if line.startswith("📦"):
                name = line.lstrip("📦").strip()
            lowered = line.lower()
            if lowered.startswith("item code:"):
                code = line.split(":", 1)[1].strip()
            if lowered.startswith("uom:"):
                uom = line.split(":", 1)[1].strip()
        if not code:
            return False
        draft["item"] = {"code": code, "name": name or code, "uom": uom or "-"}
        draft["stage"] = "await_warehouse_message"
        self.storage.save_entry_draft(user_id, draft)
        await message.reply_text(f"{name or code} tanlandi.")
        await self._prompt_entry_warehouse(
            user_id=user_id,
            chat_id=draft.get("chat_id", message.chat_id),
            api_key=api_key,
            api_secret=api_secret,
            draft=draft,
            context=context,
        )
        return True

    async def _handle_entry_warehouse_message(
        self,
        *,
        user_id: int,
        message,
        text: str,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            await message.reply_text(
                "Inline menyudan ombor tanlab shu chatga yuboring. Xabar ichida ombor nomi ko'rinishi kerak."
            )
            return True
        data = self._parse_warehouse_inline(text)
        if not data:
            return False
        warehouse_name = data.get("code")
        if not warehouse_name:
            return False
        draft["warehouse"] = warehouse_name
        draft["stage"] = "await_qty"
        self.storage.save_entry_draft(user_id, draft)
        role = draft.get("warehouse_role") or "target"
        prompt = (
            "Qancha miqdorda kelganini kiriting. Masalan: 25"
            if role == "target"
            else "Qancha miqdorda chiqayotganini kiriting. Masalan: 10"
        )
        await message.reply_text(
            f"{warehouse_name} tanlandi.\n{prompt}",
            reply_markup=self._cancel_creation_markup(),
        )
        return True

    async def _handle_entry_quantity_message(
        self,
        *,
        user_id: int,
        message,
        text: str,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        normalized = text.replace(",", ".")
        try:
            qty = float(normalized)
        except ValueError:
            await message.reply_text(
                "Miqdor noto'g'ri. Masalan: 12.5\nJarayonni to'xtatish uchun 'Bekor qilish' tugmasini tanlang.",
                reply_markup=self._cancel_creation_markup(),
            )
            return True
        if qty <= 0:
            await message.reply_text(
                "Miqdor musbat bo'lishi kerak.\nJarayonni to'xtatish uchun 'Bekor qilish' tugmasini tanlang.",
                reply_markup=self._cancel_creation_markup(),
            )
            return True
        draft["quantity"] = qty
        draft["stage"] = "submitting"
        self.storage.save_entry_draft(user_id, draft)
        await message.reply_text("⏳ Stock Entry yaratilmoqda...")
        await self._finalise_entry_creation(
            user_id=user_id,
            draft=draft,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
        )
        return True

    async def _finalise_entry_creation(
        self,
        *,
        user_id: int,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        item = draft.get("item")
        warehouse = draft.get("warehouse")
        qty = draft.get("quantity")
        entry_type = draft.get("entry_type")
        warehouse_role = draft.get("warehouse_role")
        chat_id = draft.get("chat_id", user_id)
        if not (item and warehouse and qty and entry_type and warehouse_role):
            await context.bot.send_message(
                chat_id=chat_id,
                text="Jarayon ma'lumotlari yetarli emas. Iltimos, /entry orqali qaytadan boshlang.",
            )
            self.storage.delete_entry_draft(user_id)
            return
        success, error_detail, docname = await self._create_stock_entry(
            api_key,
            api_secret,
            stock_entry_type=entry_type,
            warehouse_role=warehouse_role,
            warehouse=warehouse,
            item=item,
            quantity=qty,
        )
        if success:
            self.storage.delete_entry_draft(user_id)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "✅ Stock Entry yaratildi.\n"
                    f"Nom: {docname or 'ERPNext'}\n"
                    f"Tur: {draft.get('entry_type_label')}\n"
                    f"Item: {item.get('name')} ({item.get('code')})\n"
                    f"Ombor: {warehouse}\n"
                    f"Miqdor: {qty}"
                ),
            )
        else:
            draft["stage"] = "await_qty"
            self.storage.save_entry_draft(user_id, draft)
            message = self._format_entry_error(error_detail)
            await context.bot.send_message(
                chat_id=chat_id,
                text=message
                + "\n\nYangi miqdor yuboring yoki jarayonni to'xtatish uchun 'Bekor qilish' tugmasidan foydalaning.",
                reply_markup=self._cancel_creation_markup(),
            )

    # -------------------------------------------------------------- handlers
    async def _cancel_active_draft(
        self,
        *,
        user_id: int,
        chat_id: int,
        message,
        context: ContextTypes.DEFAULT_TYPE,
        entry_draft: Optional[Dict[str, Any]] = None,
    ) -> bool:
        draft = entry_draft or self.storage.get_entry_draft(user_id)
        if not draft:
            self._log_event(user_id, "cancel_draft_missing")
            await message.reply_text("Bekor qiladigan jarayon topilmadi.")
            return False
        draft_kind = draft.get("kind") or "stock_entry"
        stage = draft.get("stage")
        self._log_event(user_id, "cancel_draft", kind=draft_kind, stage=stage)
        if draft_kind == "purchase_confirm":
            self.storage.delete_entry_draft(user_id)
            await message.reply_text("Purchase Receipt tasdiqlash jarayoni bekor qilindi.")
            return True
        if draft_kind == "delivery_confirm":
            self.storage.delete_entry_draft(user_id)
            await message.reply_text("Delivery Note tasdiqlash jarayoni bekor qilindi.")
            return True
        notice = None
        if draft_kind == "purchase_receipt":
            notice = "Purchase Receipt jarayoni bekor qilindi."
        elif draft_kind == "delivery_note":
            notice = "Delivery Note jarayoni bekor qilindi."
        await self._cancel_entry_creation(
            user_id=user_id,
            chat_id=chat_id,
            context=context,
            notice=notice,
        )
        self._log_event(user_id, "cancel_entry_flow", kind=draft_kind, stage=stage)
        return True

    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user or chat.type != ChatType.PRIVATE:
            if chat and chat.type != ChatType.PRIVATE and update.message:
                await update.message.reply_text("Iltimos, men bilan shaxsiy chatda gaplashing: /start")
            return

        self.storage.record_user(
            user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
        )
        creds = self.storage.get_credentials(user.id)
        status = (creds or {}).get("status") or "pending_key"
        if status == "pending_key":
            text = (
                "👋 Assalomu alaykum!\n"
                "Siz uchun ERPNext stock paneli tayyor. Davom etish uchun 14-18 belgilik API kalitni yuboring.\n"
                "Masalan: AB12CD34EF56GH78"
            )
            reply_markup = self._main_menu_markup()
        elif status == "pending_secret":
            text = (
                "🔐 API kalit saqlandi.\n"
                "Endi xuddi shunday uzunlikdagi API secret ni yuboring. Masalan: JKLMNOPQ7890ABCD"
            )
            reply_markup = self._main_menu_markup()
        else:
            text = (
                "✅ API kalit va secret tasdiqlandi.\n"
                "Quyidagi menyudan foydalanib ERPNext dagi Itemlarni ko'ring yoki tayyorlab qo'ying."
            )
            reply_markup = self._main_menu_markup()

        await context.bot.send_message(chat_id=chat.id, text=text, reply_markup=reply_markup)
        if status == "active":
            await context.bot.send_message(
                chat_id=chat.id,
                text="📦 Itemlar bo'limini ochish uchun tugmani bosing.",
                reply_markup=self._items_markup(),
            )

    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if not chat:
            return
        text = (
            "Jarayon:\n"
            "1. /start ni shaxsiy chatda yuboring.\n"
            "2. 14-18 belgidan iborat API kalitni, keyin secretni kiriting.\n"
            "3. Tasdiqdan so'ng bot \"📦 Itemlarni ko'rish\" tugmasini yuboradi. "
            "Tugma inline qidiruv oynasini ochadi va ERPNext Item ro'yxatini ko'rsatadi.\n\n"
            "Har doim yangi kalit kiritish uchun shunchaki yangi qiymatni yuboring."
        )
        if chat.type == ChatType.PRIVATE:
            await context.bot.send_message(chat_id=chat.id, text=text)
        elif update.message:
            await update.message.reply_text(text)

    async def handle_items(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user or chat.type != ChatType.PRIVATE or not update.message:
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await update.message.reply_text("Avval /start orqali API kalit va secret ni tasdiqlang.")
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        await update.message.reply_text(
            "Inline tugmani bosing va itemlarni ko'ring.",
            reply_markup=self._items_markup(),
        )
        await self._send_item_preview(
            chat_id=chat.id,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
        )

    async def handle_entry_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
        if not chat or not user or not message or chat.type != ChatType.PRIVATE:
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await message.reply_text("Avval /start orqali API kalit va secret ni tasdiqlang.")
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        await self._send_item_preview(
            chat_id=chat.id,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
        )
        await self._send_entry_preview(
            chat_id=chat.id,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
            show_message=False,
        )
        await message.reply_text(
            "Stock Entry menyusi:\nHarakatlarni ko'rish yoki yangi harakat yaratish uchun variantni tanlang.",
            reply_markup=self._entry_markup(),
        )

    async def _handle_clear_request(
        self,
        *,
        user_id: int,
        message,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        creds = self.storage.get_credentials(user_id)
        if not creds or creds.get("status") == "pending_key":
            await message.reply_text("API kalitlari hali saqlanmagan. Avval 14-18 belgilik API kalit yuboring.")
            return
        self.storage.reset_credentials(user_id)
        await message.reply_text(
            "API kalit va secret tozalandi. Iltimos, yangi 14-18 belgilik API kalitni yuboring.",
            reply_markup=self._main_menu_markup(),
        )

    async def handle_clear_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
        if not chat or not user or not message or chat.type != ChatType.PRIVATE:
            return
        await self._handle_clear_request(user_id=user.id, message=message, context=context)

    async def handle_cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        user = update.effective_user
        if not message or not user or message.chat.type != ChatType.PRIVATE:
            return
        await self._cancel_active_draft(
            user_id=user.id,
            chat_id=message.chat_id,
            message=message,
            context=context,
        )

    async def handle_item_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        payload = (query.data or "").split(":", 1)
        action = payload[1] if len(payload) == 2 else ""
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        if action in {"", "refresh"}:
            await query.answer("Yangilanmoqda…", show_alert=False)
            await self._send_item_preview(
                chat_id=query.message.chat_id if query.message else user.id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
            )
            return
        success, error, detail = await self._fetch_item_detail(api_key, api_secret, action)
        if not success:
            await query.answer(error or "Item ma'lumotini olishda xatolik.", show_alert=True)
            return
        text = self._format_item_message(detail)
        await query.answer()
        if query.message:
            await context.bot.send_message(chat_id=query.message.chat_id, text=text)
        else:
            await context.bot.send_message(chat_id=user.id, text=text)

    async def handle_entry_detail_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        payload = (query.data or "").split(":", 1)
        docname = payload[1] if len(payload) == 2 else ""
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        if docname in {"", "refresh"}:
            await query.answer("Yangilanmoqda…", show_alert=False)
            await self._send_entry_preview(
                chat_id=query.message.chat_id if query.message else user.id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
                show_message=True,
            )
            return
        success, error, detail = await self._fetch_stock_entry_detail(api_key, api_secret, docname)
        if not success:
            await query.answer(error or "Stock Entry ma'lumotini olishda xatolik.", show_alert=True)
            return
        text = self._format_stock_entry_message(detail, detail)
        await query.answer()
        reply_markup = self._entry_action_buttons(detail)
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else user.id,
            text=text,
            reply_markup=reply_markup,
        )

    async def handle_entry_create_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        if not api_key or not api_secret:
            await query.answer("API kalitlari topilmadi.", show_alert=True)
            return
        chat_id = query.message.chat_id if query.message else user.id
        self.storage.delete_entry_draft(user.id)
        await query.answer()
        await context.bot.send_message(chat_id=chat_id, text="Yangi Stock Entry yaratishni boshlaymiz.")
        await self._start_entry_creation(user_id=user.id, chat_id=chat_id, context=context)

    async def handle_entry_creation_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        parts = (query.data or "").split(":", 2)
        action = parts[1] if len(parts) > 1 else ""
        value = parts[2] if len(parts) > 2 else ""
        draft = self.storage.get_entry_draft(user.id)
        if not draft:
            await query.answer("Jarayon topilmadi. /entry orqali qayta boshlang.", show_alert=True)
            return
        chat_id = draft.get("chat_id") or (query.message.chat_id if query.message else user.id)
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""

        if action == "cancel":
            await query.answer("Jarayon bekor qilindi.", show_alert=False)
            await self._cancel_entry_creation(
                user_id=user.id,
                chat_id=chat_id,
                context=context,
            )
            return

        if action == "type":
            option = ENTRY_TYPE_OPTIONS.get(value)
            if not option:
                await query.answer("Noto'g'ri tur tanlandi.", show_alert=True)
                return
            draft["entry_type"] = option["value"]
            draft["entry_type_label"] = option["label"]
            draft["warehouse_role"] = option["warehouse_role"]
            draft["stage"] = "await_item_message"
            draft.pop("item", None)
            draft.pop("warehouse", None)
            self.storage.save_entry_draft(user.id, draft)
            await query.answer(f"{option['label']} tanlandi.", show_alert=False)
            await self._prompt_entry_item(
                user_id=user.id,
                chat_id=chat_id,
                api_key=api_key,
                api_secret=api_secret,
                draft=draft,
                context=context,
            )
            return

        await query.answer("Noma'lum tanlov.", show_alert=True)

    async def handle_entry_confirm_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        draft = {
            "kind": "stock_entry",
            "stage": "await_approve",
            "chat_id": query.message.chat_id if query.message else user.id,
        }
        self.storage.save_entry_draft(user.id, draft)
        await query.answer("Inline oynani oching.", show_alert=False)
        await context.bot.send_message(
            chat_id=draft["chat_id"],
            text="Tasdiqlash uchun quyidagi oynani ochib qidiruvdan foydalaning.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📋 Tasdiqlash oynasini ochish",
                            switch_inline_query_current_chat="entryapprove",
                        )
                    ]
                ]
            ),
        )

    async def handle_entry_approve_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        docname = (query.data or "").split(":", 1)[1:]
        docname = docname[0] if docname else ""
        if not docname:
            await query.answer("Harakat aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._submit_stock_entry(
            creds.get("api_key") or "",
            creds.get("api_secret") or "",
            docname,
        )
        if success:
            await query.answer("Tasdiqlandi.", show_alert=False)
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"✅ {docname} tasdiqlandi.",
            )
        else:
            await query.answer("Xatolik yuz berdi.", show_alert=True)
            fallback = error_detail or "Noma'lum"
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"Tasdiqlashda xatolik:\n{fallback}",
            )

    async def handle_entry_cancel_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        docname = (query.data or "").split(":", 1)[1:]
        docname = docname[0] if docname else ""
        if not docname:
            await query.answer("Harakat aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._cancel_stock_entry(
            creds.get("api_key") or "",
            creds.get("api_secret") or "",
            docname,
        )
        if success:
            await query.answer("Bekor qilindi.", show_alert=False)
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"❌ {docname} bekor qilindi.",
            )
            self._log_event(user.id, "entry_cancelled", doc=docname)
        else:
            await query.answer("Xatolik yuz berdi.", show_alert=True)
            fallback = self._format_action_error(f"{docname} ni bekor qilish", error_detail)
            logger.warning("Failed to cancel stock entry %s for %s: %s", docname, user.id, fallback)
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=fallback,
            )

    async def handle_entry_delete_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        user = query.from_user
        if not user:
            await query.answer("Foydalanuvchi aniqlanmadi.", show_alert=True)
            return
        docname = (query.data or "").split(":", 1)
        docname = docname[1] if len(docname) == 2 else ""
        if not docname:
            await query.answer("Harakat aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._delete_stock_entry(
            creds.get("api_key") or "",
            creds.get("api_secret") or "",
            docname,
        )
        if success:
            await query.answer("O'chirildi.", show_alert=False)
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"🗑️ {docname} o'chirildi.",
            )
            self._log_event(user.id, "entry_deleted", doc=docname)
        else:
            await query.answer("Xatolik yuz berdi.", show_alert=True)
            fallback = self._format_action_error(f"{docname} ni o'chirish", error_detail)
            logger.warning("Failed to delete stock entry %s for %s: %s", docname, user.id, fallback)
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=fallback,
            )

    async def handle_private_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        message = update.message
        user = update.effective_user
        if not message or not user or message.chat.type != ChatType.PRIVATE:
            return
        text = (message.text or "").strip()
        if not text:
            return
        from_inline_result = bool(message.via_bot and context.bot and message.via_bot.id == context.bot.id)
        normalized = text.lower()

        self.storage.record_user(
            user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
        )
        creds = self.storage.get_credentials(user.id)
        status = (creds or {}).get("status") or "pending_key"
        entry_draft = self.storage.get_entry_draft(user.id)
        stage_label = entry_draft.get("stage") if entry_draft else "-"
        preview = self._safe_text_preview(text)
        self._log_event(
            user.id,
            "private_message",
            status=status,
            stage=stage_label,
            inline=int(from_inline_result),
            text=preview,
        )
        if from_inline_result and not entry_draft:
            return
        if entry_draft:
            draft_kind = entry_draft.get("kind") or "stock_entry"
            entry_draft["kind"] = draft_kind
            if normalized in {"/cancel", "cancel", "bekor", "bekor qilish"}:
                await self._cancel_active_draft(
                    user_id=user.id,
                    chat_id=message.chat_id,
                    message=message,
                    context=context,
                    entry_draft=entry_draft,
                )
                return
            stage = entry_draft.get("stage")
            api_key = (creds or {}).get("api_key") or ""
            api_secret = (creds or {}).get("api_secret") or ""
            if draft_kind == "purchase_confirm":
                if stage == "await_purchase_confirm":
                    handled = await self._handle_purchase_approve_message(
                        user_id=user.id,
                        message=message,
                        text=text,
                        api_key=api_key,
                        api_secret=api_secret,
                        context=context,
                    )
                    if handled:
                        return
                if from_inline_result:
                    return
                await message.reply_text(
                    (
                        "Purchase Receipt tasdiqlash jarayoni davom etmoqda.\n"
                        "Inline menyudan #purchaseapprove rezultati yuboring yoki /cancel deb yozib bekor qiling."
                    )
                )
                return
            if draft_kind == "delivery_confirm":
                if stage == "await_delivery_confirm":
                    handled = await self._handle_delivery_approve_message(
                        user_id=user.id,
                        message=message,
                        text=text,
                        api_key=api_key,
                        api_secret=api_secret,
                        context=context,
                    )
                    if handled:
                        return
                if from_inline_result:
                    return
                await message.reply_text(
                    (
                        "Delivery Note tasdiqlash jarayoni davom etmoqda.\n"
                        "Inline menyundan #deliveryapprove rezultati yuboring yoki /cancel deb yozib bekor qiling."
                    )
                )
                return
            if draft_kind == "purchase_receipt":
                handled = await self._handle_purchase_receipt_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    draft=entry_draft,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                    from_inline_result=from_inline_result,
                )
                if handled or from_inline_result:
                    return
                # purchase receipt jarayoni uchun boshqa ishlov yo'q
                return
            if draft_kind == "delivery_note":
                handled = await self._handle_delivery_note_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    draft=entry_draft,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                    from_inline_result=from_inline_result,
                )
                if handled or from_inline_result:
                    return
                return
            if stage in {"await_item_message", "await_warehouse_message", "await_qty"} and (
                not api_key or not api_secret or status != "active"
            ):
                await message.reply_text("Avval API kalit va secretni kiriting.")
                return
            if stage == "await_item_message":
                handled = await self._handle_entry_item_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    draft=entry_draft,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                )
                if handled:
                    return
            if stage == "await_warehouse_message":
                handled = await self._handle_entry_warehouse_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    draft=entry_draft,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                )
                if handled:
                    return
            if stage == "await_qty":
                handled = await self._handle_entry_quantity_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    draft=entry_draft,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                )
                if handled:
                    return
            if stage == "await_approve":
                handled = await self._handle_entry_approve_message(
                    user_id=user.id,
                    message=message,
                    text=text,
                    api_key=api_key,
                    api_secret=api_secret,
                    context=context,
                )
                if handled:
                    return
            if from_inline_result:
                return

        normalized = text.lower()
        if text in {"📦 Itemlar", "📦 Itemlarni ko'rish"}:
            if status != "active":
                await message.reply_text("Avval API kalit va secretni kiriting.")
                return
            api_key = creds.get("api_key") or ""
            api_secret = creds.get("api_secret") or ""
            await self._send_item_preview(
                chat_id=message.chat_id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
            )
            return
        if normalized in {"📋 harakatlar", "harakatlar"} or text in {"📋 Harakatlar"}:
            if status != "active":
                await message.reply_text("Avval API kalit va secretni kiriting.")
                return
            await message.reply_text(
                "Stock Entry menyusi:",
                reply_markup=self._entry_markup(),
            )
            api_key = creds.get("api_key") or ""
            api_secret = creds.get("api_secret") or ""
            await self._send_entry_preview(
                chat_id=message.chat_id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
                show_message=False,
            )
            return
        if text in {"🧾 Purchase Receipt"} or normalized in {"purchase receipt", "purchase"}:
            if status != "active":
                await message.reply_text("Avval API kalit va secretni kiriting.")
                return
            api_key = creds.get("api_key") or ""
            api_secret = creds.get("api_secret") or ""
            await self._send_purchase_preview(
                chat_id=message.chat_id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
            )
            await message.reply_text(
                "Purchase Receipt menyusi:",
                reply_markup=self._purchase_markup(),
            )
            return
        if text in {"🚚 Delivery Note"} or normalized in {"delivery note", "delivery"}:
            if status != "active":
                await message.reply_text("Avval API kalit va secretni kiriting.")
                return
            api_key = creds.get("api_key") or ""
            api_secret = creds.get("api_secret") or ""
            await self._send_delivery_preview(
                chat_id=message.chat_id,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
            )
            await message.reply_text(
                "Delivery Note menyusi:",
                reply_markup=self._delivery_markup(),
            )
            return
        if text in {"♻️ API ni tozalash"}:
            await self._handle_clear_request(user_id=user.id, message=message, context=context)
            return

        if status == "pending_key" or not (creds and creds.get("api_key")):
            if not self._validate_token(text):
                await message.reply_text(
                    "API kalit formati noto'g'ri. Faqat 14 dan 18 tagacha harf/raqam bo'lishi kerak (masalan: AB12CD34EF56GH78)."
                )
                return
            self.storage.store_api_key(user.id, text)
            await message.reply_text("API kalit saqlandi. Endi API secret yuboring (yana 14-18 belgi).")
            return

        if status == "pending_secret" or not creds.get("api_secret"):
            if not self._validate_token(text):
                await message.reply_text(
                    "API secret formati noto'g'ri. Faqat 14 dan 18 tagacha harf/raqam bo'lishi kerak (masalan: JKLMNOPQ7890ABCD)."
                )
                return
            api_key = creds.get("api_key") or ""
            verified, detail = await self._verify_credentials(api_key, text)
            self.storage.store_api_secret(user.id, text, verified=verified)
            if verified:
                await message.reply_text(
                    "✅ API secret tasdiqlandi. Endi itemlarni ko'rishingiz mumkin.",
                    reply_markup=self._main_menu_markup(),
                )
                await message.reply_text(
                    "📦 Itemlar menyusi:",
                    reply_markup=self._items_markup(),
                )
            else:
                extra = f"\nMa'lumot: {detail}" if detail else ""
                await message.reply_text("API secret saqlandi, ammo ERPNext bilan ulanish amalga oshmadi." + extra)
            return

        await message.reply_text(
            "API kalitlari allaqachon saqlangan. Item ro'yxati uchun /items yozing yoki pastdagi menyudan foydalaning.",
            reply_markup=self._main_menu_markup(),
        )
        await message.reply_text(
            "📦 Itemlar menyusi:",
            reply_markup=self._items_markup(),
        )

    async def handle_inline_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        inline_query = update.inline_query
        if not inline_query:
            return
        user = inline_query.from_user
        if not user:
            await inline_query.answer([], cache_time=5, is_personal=True)
            return

        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await inline_query.answer(
                [],
                is_personal=True,
                cache_time=3,
                button=self._inline_start_button("Avval /start ni bosing"),
            )
            return

        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""
        query_text = (inline_query.query or "").strip()
        trimmed_query = query_text.lstrip()
        lower_query = trimmed_query.lower()
        entry_item_mode = lower_query.startswith("entryitem") or lower_query.startswith("itemlookup")
        entry_wh_mode = lower_query.startswith("entrywarehouse") or lower_query.startswith("warehouse")
        entry_approve_mode = any(lower_query.startswith(prefix) for prefix in ENTRY_APPROVE_QUERY_PREFIXES)
        purchase_approve_mode = any(lower_query.startswith(prefix) for prefix in PURCHASE_APPROVE_QUERY_PREFIXES)
        purchase_mode = lower_query.startswith(PURCHASE_TRIGGER) and not purchase_approve_mode
        purchase_item_mode = lower_query.startswith(PURCHASE_ITEM_QUERY_PREFIX)
        supplier_mode = lower_query.startswith(PURCHASE_SUPPLIER_QUERY_PREFIX)
        delivery_approve_mode = any(lower_query.startswith(prefix) for prefix in DELIVERY_APPROVE_QUERY_PREFIXES)
        delivery_mode = lower_query.startswith(DELIVERY_TRIGGER) and not delivery_approve_mode
        delivery_item_mode = lower_query.startswith(DELIVERY_ITEM_QUERY_PREFIX)
        customer_mode = lower_query.startswith(DELIVERY_CUSTOMER_QUERY_PREFIX)
        entry_mode = lower_query.startswith(ENTRY_TRIGGER) and not (
            entry_item_mode or entry_wh_mode or entry_approve_mode
        )
        logger.info(
            "Inline query from %s (%s): %r",
            user.id,
            "entry" if entry_mode else "item",
            inline_query.query,
        )

        if entry_mode:
            search_term = trimmed_query[len(ENTRY_TRIGGER):].strip()
            success, error_detail, rows = await self._fetch_stock_entries(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Stock Entry ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return

            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                detail_success, _, detail = await self._fetch_stock_entry_detail(
                    api_key, api_secret, docname
                )
                message_text = self._format_stock_entry_message(row, detail if detail_success else None)
                title = f"{docname} ({row.get('purpose') or '-'})"
                warehouses = f"{row.get('from_warehouse') or '-'} → {row.get('to_warehouse') or '-'}"
                posting = row.get("posting_date") or "-"
                description = f"{posting} • {warehouses}"
                results.append(
                    InlineQueryResultArticle(
                        id=f"entry-{idx}",
                        title=title,
                        description=description,
                        input_message_content=InputTextMessageContent(message_text),
                    )
                )
                if len(results) >= min(self.config.item_limit, 10):
                    break

            await inline_query.answer(results, cache_time=0, is_personal=True)
            return
        elif entry_item_mode:
            prefix = "entryitem" if lower_query.startswith("entryitem") else "itemlookup"
            search_term = trimmed_query[len(prefix):].strip()
            success, error_detail, rows = await self._fetch_items(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Item ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                item_name = row.get("item_name") or row.get("name") or row.get("item_code") or "Item"
                item_code = row.get("item_code") or row.get("name") or "-"
                uom = row.get("stock_uom") or "-"
                text = "\n".join(
                    [
                        "#entryitem",
                        f"📦 {item_name}",
                        f"Item Code: {item_code}",
                        f"UOM: {uom}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"entryitem-{idx}",
                        title=f"{item_name} ({item_code})",
                        description=f"UOM: {uom}",
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.item_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif entry_wh_mode:
            prefix = "entrywarehouse" if lower_query.startswith("entrywarehouse") else "warehouse"
            search_term = trimmed_query[len(prefix):].strip()
            success, error_detail, rows = await self._fetch_warehouses(
                api_key,
                api_secret,
                limit=self.config.warehouse_limit,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Ombor ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            lowered_term = search_term.lower()
            for idx, row in enumerate(rows):
                name = row.get("name") or "-"
                label = row.get("warehouse_name") or name
                if lowered_term and lowered_term not in label.lower() and lowered_term not in name.lower():
                    continue
                text = "\n".join(
                    [
                        "#entrywarehouse",
                        f"Warehouse: {label}",
                        f"Code: {name}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"entrywarehouse-{idx}",
                        title=label,
                        description=name,
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.warehouse_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif entry_approve_mode:
            active_prefix = next((prefix for prefix in ENTRY_APPROVE_QUERY_PREFIXES if lower_query.startswith(prefix)), "")
            search_term = trimmed_query[len(active_prefix):].strip()
            success, error_detail, rows = await self._fetch_stock_entries(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Stock Entry ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                status = self._docstatus_label(row.get("docstatus"))
                title = f"{docname} ({status})"
                description = row.get("posting_date") or "-"
                track_token = f"{ENTRY_APPROVE_PREFIX}:{docname}"
                text = "\n".join(
                    [
                        "#entryapprove",
                        f"Stock Entry: {docname}",
                        f"Status: {status}",
                        track_token,
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"entryapprove-{idx}",
                        title=title,
                        description=description,
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= min(self.config.item_limit, 10):
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif purchase_mode:
            search_term = trimmed_query[len(PURCHASE_TRIGGER):].strip()
            success, error_detail, rows = await self._fetch_purchase_receipts(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Purchase Receipt ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                detail_success, _, detail = await self._fetch_purchase_receipt_detail(
                    api_key, api_secret, docname
                )
                message_text = self._format_purchase_receipt_message(
                    row, detail if detail_success else None
                )
                supplier = row.get("supplier") or "-"
                posting_date = row.get("posting_date") or "-"
                posting_time = row.get("posting_time") or "-"
                description = f"{supplier} • {posting_date} {posting_time}"
                results.append(
                    InlineQueryResultArticle(
                        id=f"purchase-{idx}",
                        title=f"{docname} ({supplier})",
                        description=description,
                        input_message_content=InputTextMessageContent(message_text),
                    )
                )
                if len(results) >= min(self.config.purchase_receipt_limit, 10):
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif purchase_approve_mode:
            active_prefix = next(
                (prefix for prefix in PURCHASE_APPROVE_QUERY_PREFIXES if lower_query.startswith(prefix)), ""
            )
            search_term = trimmed_query[len(active_prefix):].strip()
            success, error_detail, rows = await self._fetch_purchase_receipts(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Purchase Receipt ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                status = self._docstatus_label(row.get("docstatus"))
                supplier = row.get("supplier") or "-"
                posting = row.get("posting_date") or "-"
                track_token = f"{PURCHASE_APPROVE_PREFIX}:{docname}"
                text = "\n".join(
                    [
                        "#purchaseapprove",
                        f"Purchase Receipt: {docname}",
                        f"Supplier: {supplier}",
                        f"Status: {status}",
                        track_token,
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"purchaseapprove-{idx}",
                        title=f"{docname} ({status})",
                        description=f"{supplier} • {posting}",
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= min(self.config.purchase_receipt_limit, 10):
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif purchase_item_mode:
            search_term = trimmed_query[len(PURCHASE_ITEM_QUERY_PREFIX):].strip()
            success, error_detail, rows = await self._fetch_items(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Item ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                item_name = row.get("item_name") or row.get("name") or row.get("item_code") or "Item"
                item_code = row.get("item_code") or row.get("name") or "-"
                uom = row.get("stock_uom") or "-"
                text = "\n".join(
                    [
                        "#pritem",
                        f"📦 {item_name}",
                        f"Item Code: {item_code}",
                        f"UOM: {uom}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"purchaseitem-{idx}",
                        title=f"{item_name} ({item_code})",
                        description=f"UOM: {uom}",
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.item_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif supplier_mode:
            search_term = trimmed_query[len(PURCHASE_SUPPLIER_QUERY_PREFIX):].strip()
            success, error_detail, rows = await self._fetch_suppliers(
                api_key,
                api_secret,
                limit=self.config.supplier_limit,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Supplier ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            lowered_term = search_term.lower()
            for idx, row in enumerate(rows):
                name = row.get("name") or "-"
                label = row.get("supplier_name") or row.get("supplier_group") or name
                if lowered_term and lowered_term not in name.lower() and lowered_term not in label.lower():
                    continue
                text = "\n".join(
                    [
                        "#supplier",
                        f"Supplier: {label}",
                        f"Code: {name}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"supplier-{idx}",
                        title=label,
                        description=name,
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.supplier_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif delivery_mode:
            search_term = trimmed_query[len(DELIVERY_TRIGGER):].strip()
            success, error_detail, rows = await self._fetch_delivery_notes(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Delivery Note ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return

            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                detail_success, _, detail = await self._fetch_delivery_note_detail(
                    api_key, api_secret, docname
                )
                message_text = self._format_delivery_note_message(row, detail if detail_success else None)
                title = f"{docname} ({row.get('customer') or '-'})"
                posting = row.get("posting_date") or "-"
                description = f"{posting} • {row.get('customer') or '-'}"
                results.append(
                    InlineQueryResultArticle(
                        id=f"delivery-{idx}",
                        title=title,
                        description=description,
                        input_message_content=InputTextMessageContent(message_text),
                    )
                )
                if len(results) >= min(self.config.delivery_note_limit, 10):
                    break

            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif delivery_approve_mode:
            active_prefix = next(
                (prefix for prefix in DELIVERY_APPROVE_QUERY_PREFIXES if lower_query.startswith(prefix)), ""
            )
            search_term = trimmed_query[len(active_prefix):].strip()
            success, error_detail, rows = await self._fetch_delivery_notes(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Delivery Note ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                docname = row.get("name")
                if not docname:
                    continue
                status = self._docstatus_label(row.get("docstatus"))
                customer = row.get("customer") or "-"
                posting = row.get("posting_date") or "-"
                track_token = f"{DELIVERY_APPROVE_PREFIX}:{docname}"
                text = "\n".join(
                    [
                        "#deliveryapprove",
                        f"Delivery Note: {docname}",
                        f"Customer: {customer}",
                        f"Status: {status}",
                        track_token,
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"deliveryapprove-{idx}",
                        title=f"{docname} ({status})",
                        description=f"{customer} • {posting}",
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= min(self.config.delivery_note_limit, 10):
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif delivery_item_mode:
            search_term = trimmed_query[len(DELIVERY_ITEM_QUERY_PREFIX):].strip()
            success, error_detail, rows = await self._fetch_items(
                api_key,
                api_secret,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Item ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            for idx, row in enumerate(rows):
                item_name = row.get("item_name") or row.get("name") or row.get("item_code") or "Item"
                item_code = row.get("item_code") or row.get("name") or "-"
                uom = row.get("stock_uom") or "-"
                text = "\n".join(
                    [
                        "#dnitem",
                        f"📦 {item_name}",
                        f"Item Code: {item_code}",
                        f"UOM: {uom}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"deliveryitem-{idx}",
                        title=f"{item_name} ({item_code})",
                        description=f"UOM: {uom}",
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.item_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        elif customer_mode:
            search_term = trimmed_query[len(DELIVERY_CUSTOMER_QUERY_PREFIX):].strip()
            success, error_detail, rows = await self._fetch_customers(
                api_key,
                api_secret,
                limit=self.config.customer_limit,
                query=search_term,
            )
            if not success:
                hint = error_detail or "Customer ro'yxatini olishda xatolik"
                await inline_query.answer(
                    [],
                    is_personal=True,
                    cache_time=3,
                    button=self._inline_start_button(hint[:48]),
                )
                return
            results = []
            lowered_term = search_term.lower()
            for idx, row in enumerate(rows):
                name = row.get("name") or "-"
                label = row.get("customer_name") or row.get("customer_group") or name
                if lowered_term and lowered_term not in name.lower() and lowered_term not in label.lower():
                    continue
                text = "\n".join(
                    [
                        "#customer",
                        f"Customer: {label}",
                        f"Code: {name}",
                    ]
                )
                results.append(
                    InlineQueryResultArticle(
                        id=f"customer-{idx}",
                        title=label,
                        description=name,
                        input_message_content=InputTextMessageContent(text),
                    )
                )
                if len(results) >= self.config.customer_limit:
                    break
            await inline_query.answer(results, cache_time=0, is_personal=True)
            return

        query_text = trimmed_query
        if lower_query in {"items", "bot items"}:
            query_text = ""
        success, error_detail, rows = await self._fetch_items(
            api_key,
            api_secret,
            query=query_text,
        )
        if not success:
            hint = error_detail or "ERPNext bilan aloqa yo'q"
            await inline_query.answer(
                [],
                is_personal=True,
                cache_time=3,
                button=self._inline_start_button(hint[:48]),
            )
            return

        results = []
        for idx, row in enumerate(rows):
            item_name = row.get("item_name") or row.get("name") or row.get("item_code") or "Item"
            item_code = row.get("item_code") or row.get("name") or ""
            uom = row.get("stock_uom") or "-"
            item_group = row.get("item_group") or "-"
            rate = row.get("standard_rate")
            description_raw = self._clean_text(row.get("description"))
            title = f"{item_name} ({item_code})" if item_code and item_code != item_name else item_name
            summary_parts = [f"Kod: {item_code or '-'}", f"UOM: {uom}", f"Group: {item_group}"]
            description = " | ".join(summary_parts)
            detail_lines = [
                f"📦 {title}",
                f"Item Code: {item_code or '-'}",
                f"Item Group: {item_group}",
                f"UOM: {uom}",
            ]
            if rate not in (None, ""):
                detail_lines.append(f"Narx: {rate}")
            if description_raw:
                detail_lines.append(f"Ta'rif: {description_raw[:300]}")
            text = "\n".join(detail_lines)
            results.append(
                InlineQueryResultArticle(
                    id=f"item-{idx}",
                    title=title,
                    description=description,
                    input_message_content=InputTextMessageContent(text),
                )
            )
            if len(results) >= self.config.item_limit:
                break

        await inline_query.answer(results, cache_time=0, is_personal=True)

    async def handle_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.exception("Stock bot error: %s", context.error)

    # ------------------------------------------------------------ ERP helpers
    async def _verify_credentials(self, api_key: str, api_secret: str) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}{self.config.verify_endpoint}"

        def _request() -> Tuple[bool, Optional[str]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, timeout=10)
            if 200 <= response.status_code < 300:
                return True, None
            try:
                data = response.json()
                detail = data.get("message") or data.get("exception") or str(data)
            except ValueError:
                detail = response.text
            return False, f"HTTP {response.status_code}: {detail}"

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Credential verification failed: %s", exc)
            return False, str(exc)

    async def _fetch_items(
        self,
        api_key: str,
        api_secret: str,
        *,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Item"
        params = {
            "fields": json.dumps(
                ["name", "item_code", "item_name", "item_group", "stock_uom", "description", "standard_rate"]
            ),
            "limit_page_length": str(self.config.item_limit),
            "order_by": "item_name asc",
        }
        query = query.strip()
        if query:
            filters = [["Item", "item_name", "like", f"%{query}%"]]
            params["filters"] = json.dumps(filters)

        def _request() -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", []
            try:
                payload = response.json()
            except ValueError:
                return False, "ERPNext javobini JSON tarzida o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ERPNext itemlarini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_warehouses(
        self,
        api_key: str,
        api_secret: str,
        *,
        limit: int = 25,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Warehouse"
        params = {
            "fields": json.dumps(["name", "warehouse_name"]),
            "limit_page_length": str(limit),
            "order_by": "warehouse_name asc",
        }
        query = (query or "").strip()
        if query:
            filters = [["Warehouse", "warehouse_name", "like", f"%{query}%"]]
            params["filters"] = json.dumps(filters)

        def _request() -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", []
            try:
                payload = response.json()
            except ValueError:
                return False, "Warehouse javobini JSON tarzida o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Warehouse ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_suppliers(
        self,
        api_key: str,
        api_secret: str,
        *,
        limit: int = 25,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Supplier"
        params = {
            "fields": json.dumps(["name", "supplier_name", "supplier_group"]),
            "limit_page_length": str(limit),
            "order_by": "supplier_name asc",
        }
        query = (query or "").strip()
        if query:
            params["filters"] = json.dumps([["Supplier", "supplier_name", "like", f"%{query}%"]])

        def _request() -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", []
            try:
                payload = response.json()
            except ValueError:
                return False, "Supplier javobini JSON tarzida o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Supplier ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_stock_entries(
        self,
        api_key: str,
        api_secret: str,
        *,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/{quote('Stock Entry', safe='')}"
        params = {
            "fields": json.dumps(
                [
                    "name",
                    "purpose",
                    "stock_entry_type",
                    "posting_date",
                    "posting_time",
                    "from_warehouse",
                    "to_warehouse",
                    "total_outgoing_value",
                    "total_incoming_value",
                    "docstatus",
                ]
            ),
            "limit_page_length": str(min(self.config.item_limit, 15)),
            "order_by": "posting_date desc",
        }
        if query:
            params["filters"] = json.dumps(
                [["Stock Entry", "name", "like", f"%{query}%"]]
            )

        def _request() -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", []
            try:
                payload = response.json()
            except ValueError:
                return False, "ERPNext javobini JSON tarzida o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_stock_entry_detail(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Stock Entry/{quote(docname, safe='')}"

        def _request() -> Tuple[bool, Optional[str], Dict[str, Any]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", {}
            try:
                payload = response.json()
            except ValueError:
                return False, "Stock Entry ma'lumotini JSON sifatida o'qib bo'lmadi.", {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, dict):
                return False, "Stock Entry ma'lumotlari topilmadi.", {}
            return True, None, data

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry tafsilotlarini olishda xatolik: %s", exc)
            return False, str(exc), {}

    async def _fetch_item_detail(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Item/{quote(docname, safe='')}"

        def _request() -> Tuple[bool, Optional[str], Dict[str, Any]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.get(endpoint, headers=headers, timeout=10)
            if response.status_code >= 400:
                try:
                    payload = response.json()
                    detail = payload.get("message") or payload.get("exception") or str(payload)
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", {}
            try:
                payload = response.json()
            except ValueError:
                return False, "Item tafsilotini JSON tarzida o'qib bo'lmadi.", {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, dict):
                return False, "Item ma'lumoti topilmadi.", {}
            return True, None, data

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Item tafsilotlarini olishda xatolik: %s", exc)
            return False, str(exc), {}

    async def _create_stock_entry(
        self,
        api_key: str,
        api_secret: str,
        *,
        stock_entry_type: str,
        warehouse_role: str,
        warehouse: str,
        item: Dict[str, Any],
        quantity: float,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Stock Entry"
        uom = item.get("uom") or item.get("stock_uom") or "Nos"
        item_payload = {
            "item_code": item.get("code"),
            "item_name": item.get("name"),
            "qty": quantity,
            "uom": uom,
            "stock_uom": uom,
        }
        if warehouse_role == "target":
            item_payload["t_warehouse"] = warehouse
        else:
            item_payload["s_warehouse"] = warehouse
        payload = {
            "company": self.config.default_company,
            "stock_entry_type": stock_entry_type,
            "items": [item_payload],
            "naming_series": self.config.entry_series_template,
        }
        if warehouse_role == "target":
            payload["to_warehouse"] = warehouse
        else:
            payload["from_warehouse"] = warehouse

        def _request() -> Tuple[bool, Optional[str], Optional[str]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.post(endpoint, headers=headers, json=payload, timeout=15)
            if response.status_code >= 400:
                try:
                    body = response.json()
                    detail = (
                        body.get("message")
                        or body.get("exception")
                        or body.get("_server_messages")
                        or str(body)
                    )
                except ValueError:
                    detail = response.text
                return False, f"HTTP {response.status_code}: {detail}", None
            try:
                data = response.json().get("data")
                docname = data.get("name") if isinstance(data, dict) else None
            except Exception:  # noqa: BLE001
                docname = None
            return True, None, docname

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry yaratishda xatolik: %s", exc)
            return False, str(exc), None

    async def _submit_stock_entry(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/method/run_doc_method"

        def _request() -> Tuple[bool, Optional[str]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            payload = {"dt": "Stock Entry", "dn": docname, "method": "submit"}
            response = requests.post(endpoint, headers=headers, json=payload, timeout=15)
            if response.status_code >= 400:
                try:
                    body = response.json()
                    detail = body.get("message") or body.get("exception") or str(body)
                except ValueError:
                    detail = response.text
                return False, detail
            return True, None

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry submitda xatolik: %s", exc)
            return False, str(exc)

    async def _cancel_stock_entry(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/method/run_doc_method"

        def _request() -> Tuple[bool, Optional[str]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            payload = {"dt": "Stock Entry", "dn": docname, "method": "cancel"}
            response = requests.post(endpoint, headers=headers, json=payload, timeout=15)
            if response.status_code >= 400:
                try:
                    body = response.json()
                    detail = body.get("message") or body.get("exception") or str(body)
                except ValueError:
                    detail = response.text
                return False, detail
            return True, None

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry cancelda xatolik: %s", exc)
            return False, str(exc)

    async def _delete_stock_entry(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Stock Entry/{quote(docname, safe='')}"

        def _request() -> Tuple[bool, Optional[str]]:
            headers = {
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            }
            response = requests.delete(endpoint, headers=headers, timeout=15)
            if response.status_code >= 400:
                try:
                    body = response.json()
                    detail = body.get("message") or body.get("exception") or str(body)
                except ValueError:
                    detail = response.text
                return False, detail
            return True, None

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stock Entry ni o'chirishda xatolik: %s", exc)
            return False, str(exc)

    def _format_entry_error(self, error_detail: Optional[str]) -> str:
        if not error_detail:
            return (
                "Stock Entry yaratishda xatolik yuz berdi. ERPNext talablarini tekshirib, item ma'lumotlarini yangilang."
            )
        cleaned = self._clean_text(error_detail).replace("\n", " ").strip()
        if "Allow Zero Valuation Rate" in cleaned or "Valuation Rate" in cleaned:
            return (
                "Stock Entry yaratilmadi: ERPNext item uchun baholash (valuation) qiymatini talab qildi.\n"
                "• Item kartasida `Standard Rate` qo'shing yoki\n"
                "• Stock Entry formasi ichida “Allow Zero Valuation Rate” opsiyasini yoqing.\n"
                "Shundan so'ng bot orqali jarayonni qayta boshlang.\n"
                f"ERP xabari: {cleaned}"
            )
        return f"Stock Entry yaratishda xatolik yuz berdi.\nERP javobi: {cleaned}"

    def _format_action_error(self, action_label: str, detail: Optional[str]) -> str:
        if not detail:
            return f"{action_label} bajarilmadi. ERPNext javobi olinmadi."
        cleaned = self._clean_text(detail).replace("\n", " ").strip()
        lowered = cleaned.lower()
        if "cannot delete or cancel" in lowered:
            reason = (
                "Bu hujjat ERPNext dagi boshqa hujjatlar (masalan, GL Entry yoki boshqa Stock Entry) bilan bog'langan. "
                "Avval ularni bekor qilmasdan turib bu amaliyotni bajarib bo'lmaydi."
            )
            return f"{action_label} mumkin emas.\nSabab: {reason}\nERP xabari: {cleaned}"
        if "negativestockerror" in lowered or "negative stock" in lowered:
            reason = "Omborda yetarli qoldiq yo'q, shuning uchun ERPNext amaliyotni rad etdi."
            return f"{action_label} mumkin emas.\nSabab: {reason}\nERP xabari: {cleaned}"
        return f"{action_label} bajarilmadi.\nERP xabari: {cleaned}"

    def _format_stock_entry_message(
        self,
        summary: Dict[str, Any],
        detail: Optional[Dict[str, Any]],
    ) -> str:
        detail = detail or {}
        name = summary.get("name") or detail.get("name") or "-"
        purpose = summary.get("purpose") or detail.get("purpose") or "-"
        entry_type = summary.get("stock_entry_type") or detail.get("stock_entry_type") or "-"
        posting_date = summary.get("posting_date") or detail.get("posting_date") or "-"
        posting_time = summary.get("posting_time") or detail.get("posting_time") or "-"
        from_wh = summary.get("from_warehouse") or detail.get("from_warehouse")
        to_wh = summary.get("to_warehouse") or detail.get("to_warehouse")
        out_value = summary.get("total_outgoing_value") or detail.get("total_outgoing_value") or "-"
        in_value = summary.get("total_incoming_value") or detail.get("total_incoming_value") or "-"
        items = detail.get("items")
        if not from_wh or from_wh in {None, ""}:
            if isinstance(items, list):
                for item in items:
                    if item.get("s_warehouse"):
                        from_wh = item["s_warehouse"]
                        break
        if not to_wh or to_wh in {None, ""}:
            if isinstance(items, list):
                for item in items:
                    if item.get("t_warehouse"):
                        to_wh = item["t_warehouse"]
                        break
        from_wh = from_wh or "-"
        to_wh = to_wh or "-"
        lines = [
            f"🚚 Stock Entry: {name}",
            f"Maqsad: {purpose}",
            f"Tur: {entry_type}",
            f"Sana: {posting_date} {posting_time}",
            f"Source Warehouse: {from_wh}",
            f"Target Warehouse: {to_wh}",
            f"Qiymat: chiqish {out_value}, kirish {in_value}",
            f"Status: {self._docstatus_label(detail.get('docstatus'))}",
        ]
        if isinstance(items, list) and items:
            lines.append("")
            max_items = 10
            for item in items[:max_items]:
                item_code = item.get("item_code") or "-"
                item_name = item.get("item_name") or ""
                qty = item.get("qty")
                uom = item.get("uom") or ""
                s_wh = item.get("s_warehouse") or from_wh
                t_wh = item.get("t_warehouse") or to_wh
                qty_part = f"{qty} {uom}".strip()
                label = item_name if item_name and item_name != item_code else ""
                lines.append(
                    f"• {item_code} {label} — {qty_part or '—'} ({s_wh} → {t_wh})"
                )
            if len(items) > max_items:
                lines.append(f"... va yana {len(items) - max_items} ta pozitsiya")
        return "\n".join(lines)

    def _format_purchase_receipt_message(
        self,
        summary: Dict[str, Any],
        detail: Optional[Dict[str, Any]],
    ) -> str:
        detail = detail or {}
        name = summary.get("name") or detail.get("name") or "-"
        supplier = detail.get("supplier_name") or summary.get("supplier") or detail.get("supplier") or "-"
        posting_date = summary.get("posting_date") or detail.get("posting_date") or "-"
        posting_time = summary.get("posting_time") or detail.get("posting_time") or "-"
        warehouse = summary.get("set_warehouse") or detail.get("set_warehouse")
        items = detail.get("items")
        if not warehouse and isinstance(items, list) and items:
            warehouse = items[0].get("warehouse")
        warehouse = warehouse or "-"
        total = detail.get("grand_total") or summary.get("grand_total") or "-"
        lines = [
            f"🧾 Purchase Receipt: {name}",
            f"Supplier: {supplier}",
            f"Sana: {posting_date} {posting_time}",
            f"Ombor: {warehouse}",
            f"Jami: {total}",
            f"Status: {self._docstatus_label(detail.get('docstatus'))}",
        ]
        if isinstance(items, list) and items:
            lines.append("")
            max_items = 10
            for item in items[:max_items]:
                item_code = item.get("item_code") or "-"
                item_name = item.get("item_name") or ""
                accepted = item.get("accepted_qty") or item.get("qty") or "-"
                rejected = item.get("rejected_qty") or 0
                rate = item.get("rate") or "-"
                label = item_name if item_name and item_name != item_code else ""
                lines.append(
                    f"• {item_code} {label} — Qabul: {accepted}, Reject: {rejected}, Rate: {rate}"
                )
            if len(items) > max_items:
                lines.append(f"... va yana {len(items) - max_items} ta item")
        return "\n".join(lines)

    async def _handle_entry_approve_message(
        self,
        *,
        user_id: int,
        message,
        text: str,
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return False
        token_line = next((line for line in lines if line.startswith(f"{ENTRY_APPROVE_PREFIX}:")), None)
        if not token_line:
            return False
        _, docname = token_line.split(":", 1)
        docname = docname.strip()
        if not docname:
            return False
        success, error_detail, detail = await self._fetch_stock_entry_detail(api_key, api_secret, docname)
        if not success:
            fallback = error_detail or "Ma'lumot topilmadi."
            await message.reply_text(f"Tasdiqlashda xatolik:\n{fallback}")
            return True
        summary = detail.copy()
        text_message = self._format_stock_entry_message(summary, detail)
        markup = self._entry_action_buttons(detail)
        await message.reply_text(text_message, reply_markup=markup)
        self.storage.delete_entry_draft(user_id)
        return True

    def _entry_action_buttons(self, detail: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
        docname = detail.get("name")
        if not docname:
            return None
        docstatus = detail.get("docstatus")
        buttons: list[list[InlineKeyboardButton]] = []
        if docstatus == 0:
            buttons.append(
                [
                    InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"{ENTRY_APPROVE_PREFIX}:{docname}"),
                    InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{ENTRY_DELETE_PREFIX}:{docname}"),
                ]
            )
        elif docstatus == 1:
            buttons.append(
                [
                    InlineKeyboardButton("❌ Bekor qilish", callback_data=f"{ENTRY_CANCEL_PREFIX}:{docname}"),
                ]
            )
        else:
            buttons.append(
                [
                    InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{ENTRY_DELETE_PREFIX}:{docname}"),
                ]
            )
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def _format_item_message(detail: Dict[str, Any]) -> str:
        name = detail.get("item_name") or detail.get("name") or detail.get("item_code") or "-"
        code = detail.get("item_code") or detail.get("name") or "-"
        group = detail.get("item_group") or "-"
        uom = detail.get("stock_uom") or detail.get("uom") or "-"
        description = detail.get("description")
        standard_rate = detail.get("standard_rate")
        disabled = detail.get("disabled")
        lines = [
            f"📦 Item: {name}",
            f"Code: {code}",
            f"Group: {group}",
            f"UOM: {uom}",
        ]
        if standard_rate not in (None, ""):
            lines.append(f"Narx: {standard_rate}")
        if disabled:
            lines.append("Holat: ❌ faol emas")
        if description:
            lines.append("")
            lines.append(StockManagerBot._clean_text(description)[:600])
        return "\n".join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    config = load_config()
    bot = StockManagerBot(config)
    bot.application.run_polling(drop_pending_updates=True)


__all__ = ["StockManagerBot", "main"]
