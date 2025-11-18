from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from urllib.parse import quote
import requests
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.constants import ChatType
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

DELIVERY_TRIGGER = "delivery"
DELIVERY_CALLBACK_CREATE = "delivery:create"
DELIVERY_CREATE_PREFIX = "deliverycreate"
DELIVERY_CUSTOMER_QUERY_PREFIX = "dncustomer"
DELIVERY_ITEM_QUERY_PREFIX = "dnitem"
DELIVERY_APPROVE_PREFIX = "delivery-approve"
DELIVERY_CANCEL_PREFIX = "delivery-cancel"
DELIVERY_DELETE_PREFIX = "delivery-delete"
DELIVERY_CONFIRM_CALLBACK = "delivery:confirm"
DELIVERY_APPROVE_QUERY_PREFIXES = ("deliveryapprove", "dnapprove")
DELIVERY_DISMISS_PREFIX = "delivery-dismiss"


class DeliveryFlowMixin:
    """Handles Delivery Note flow."""

    # ---------------------------------------------------------------- menus
    def _delivery_markup(self) -> InlineKeyboardMarkup:
        view_button = InlineKeyboardButton(
            "Ko'rish",
            switch_inline_query_current_chat=DELIVERY_TRIGGER,
        )
        create_button = InlineKeyboardButton(
            "➕ Yangi rasmiylashtirish", callback_data=DELIVERY_CALLBACK_CREATE
        )
        confirm_button = InlineKeyboardButton(
            "✔️ Tasdiqlash", callback_data=DELIVERY_CONFIRM_CALLBACK
        )
        return InlineKeyboardMarkup([[view_button], [create_button], [confirm_button]])

    def _delivery_cancel_button(self) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            "❌ Jarayonni bekor qilish", callback_data=f"{DELIVERY_CREATE_PREFIX}:cancel"
        )

    def _delivery_cancel_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[self._delivery_cancel_button()]])

    def _delivery_skip_button(self) -> InlineKeyboardButton:
        return InlineKeyboardButton("⏭ O'tkazib yuborish", callback_data=f"{DELIVERY_CREATE_PREFIX}:skip")

    def _delivery_skip_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[self._delivery_skip_button()], [self._delivery_cancel_button()]])

    def _delivery_yes_no_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Ha", callback_data=f"{DELIVERY_CREATE_PREFIX}:yn:yes"),
                    InlineKeyboardButton("Yo'q", callback_data=f"{DELIVERY_CREATE_PREFIX}:yn:no"),
                ],
                [self._delivery_cancel_button()],
            ]
        )

    # ---------------------------------------------------------- prompts/flow
    async def _start_delivery_note_creation(
        self,
        *,
        user_id: int,
        chat_id: int,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        now = datetime.utcnow()
        draft = {
            "kind": "delivery_note",
            "stage": "dn_customer",
            "chat_id": chat_id,
            "series": self.config.delivery_note_series_template,
            "posting_date": now.strftime("%Y-%m-%d"),
            "posting_time": now.strftime("%H:%M"),
            "is_return": False,
            "items": [],
        }
        self.storage.save_entry_draft(user_id, draft)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🚚 Yangi chiqim hujjati (Delivery Note) seriyasi: {draft['series']}\n"
                f"Sana: {draft['posting_date']} {draft['posting_time']}\n"
                "Avval mijozni tanlang."
            ),
        )
        await self._prompt_delivery_customer(chat_id=chat_id, context=context)

    async def _prompt_delivery_customer(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👤 Mijoz qidirish",
                        switch_inline_query_current_chat=f"{DELIVERY_CUSTOMER_QUERY_PREFIX} ",
                    )
                ],
                [self._delivery_cancel_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Mijozni tanlang. Inline qidiruvdan foydalaning.",
            reply_markup=markup,
        )

    async def _prompt_delivery_posting_date(
        self,
        *,
        chat_id: int,
        current_date: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Posting sanasi hozir {current_date}.\n"
                "O'zgartirmoqchi bo'lsangiz YYYY-MM-DD formatida yuboring yoki \"O'tkazib yuborish\"ni bosing."
            ),
            reply_markup=self._delivery_skip_markup(),
        )

    async def _prompt_delivery_posting_time(
        self,
        *,
        chat_id: int,
        current_time: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Posting vaqti hozir {current_time}.\n"
                "HH:MM formatida yuboring yoki \"O'tkazib yuborish\"ni bosing."
            ),
            reply_markup=self._delivery_skip_markup(),
        )

    async def _prompt_delivery_return_choice(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Bu qaytariluvchi chiqim hujjatimi?",
            reply_markup=self._delivery_yes_no_markup(),
        )

    async def _prompt_delivery_source_warehouse(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🏬 Ombor tanlash",
                        switch_inline_query_current_chat="warehouse ",
                    )
                ],
                [self._delivery_cancel_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Qaysi ombordan chiqarilyapti?",
            reply_markup=markup,
        )

    def _delivery_items_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📦 Buyum qidirish",
                        switch_inline_query_current_chat=f"{DELIVERY_ITEM_QUERY_PREFIX} ",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "✅ Rasmiylashtirishni yakunlash", callback_data=f"{DELIVERY_CREATE_PREFIX}:finish"
                    )
                ],
                [self._delivery_cancel_button()],
            ]
        )

    async def _prompt_delivery_items_menu(
        self,
        *,
        chat_id: int,
        draft: Dict[str, Any],
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        items = draft.get("items") or []
        if items:
            lines = ["Tanlangan itemlar:"]
            for idx, item in enumerate(items, start=1):
                lines.append(
                    f"{idx}. {item.get('name')} — {item.get('qty')} {item.get('uom')} (Narx: {item.get('rate')})"
                )
            summary = "\n".join(lines)
        else:
            summary = "Hozircha item qo'shilmagan."
        await context.bot.send_message(
            chat_id=chat_id,
            text=summary + "\nYangi item qo'shish yoki hujjatni yakunlash uchun pastdagi tugmalardan foydalaning.",
            reply_markup=self._delivery_items_markup(),
        )

    # ----------------------------------------------------------- parsers
    @staticmethod
    def _parse_delivery_customer(text: str) -> Optional[Dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines or not any("#customer" in line.lower() or "customer:" in line.lower() for line in lines):
            return None
        code = None
        label = None
        for line in lines:
            lowered = line.lower()
            if lowered.startswith("customer:"):
                label = line.split(":", 1)[1].strip()
            if lowered.startswith("code:"):
                code = line.split(":", 1)[1].strip()
        if not code:
            code = label
        if not code:
            return None
        return {"code": code, "label": label or code}

    @staticmethod
    def _parse_delivery_item(text: str) -> Optional[Dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines or not any("#dnitem" in line.lower() for line in lines):
            return None
        code = None
        name = None
        uom = None
        for line in lines:
            if line.startswith("📦"):
                name = line.lstrip("📦").strip()
            lowered = line.lower()
            if lowered.startswith("item code:") or lowered.startswith("buyum kodi:"):
                code = line.split(":", 1)[1].strip()
            if lowered.startswith("uom:"):
                uom = line.split(":", 1)[1].strip()
        if not code:
            return None
        return {"code": code, "name": name or code, "uom": uom or "-"}

    @staticmethod
    def _delivery_parse_yes_no(value: str) -> Optional[bool]:
        normalized = (value or "").strip().lower()
        positives = {"ha", "ha.", "yes", "y", "true", "1"}
        negatives = {"yo'q", "yoq", "yo'q.", "no", "n", "false", "0"}
        if normalized in positives:
            return True
        if normalized in negatives:
            return False
        return None

    # ----------------------------------------------------- message handler
    async def _handle_delivery_note_message(
        self,
        *,
        user_id: int,
        message,
        text: str,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
        from_inline_result: bool,
    ) -> bool:
        stage = draft.get("stage")
        chat_id = draft.get("chat_id", message.chat_id)
        normalized = text.strip().lower()
        skip_values = {
            "skip",
            "-",
            "yo'q",
            "yoq",
            "otkaz",
            "o'tkaz",
            "otkazib yuborish",
            "o'tkazib yuborish",
        }

        if stage == "dn_customer":
            data = self._parse_delivery_customer(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Inline oynadan mijozni tanlang.")
                return True
            draft["customer"] = data
            draft["stage"] = "dn_date"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(f"{data.get('label')} tanlandi.")
            await self._prompt_delivery_posting_date(
                chat_id=chat_id, current_date=draft.get("posting_date") or "", context=context
            )
            return True

        if stage == "dn_date":
            if normalized not in skip_values:
                try:
                    parsed = datetime.strptime(text.strip(), "%Y-%m-%d")
                    draft["posting_date"] = parsed.strftime("%Y-%m-%d")
                except ValueError:
                    await message.reply_text("Sana formatini YYYY-MM-DD ko'rinishida yuboring.")
                    return True
            draft["stage"] = "dn_time"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_delivery_posting_time(
                chat_id=chat_id, current_time=draft.get("posting_time") or "", context=context
            )
            return True

        if stage == "dn_time":
            if normalized not in skip_values:
                try:
                    parsed = datetime.strptime(text.strip(), "%H:%M")
                    draft["posting_time"] = parsed.strftime("%H:%M")
                except ValueError:
                    await message.reply_text("Vaqt formatini HH:MM ko'rinishida yuboring.")
                    return True
            draft["stage"] = "dn_is_return"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_delivery_return_choice(chat_id=chat_id, context=context)
            return True

        if stage == "dn_is_return":
            decision = self._delivery_parse_yes_no(text)
            if decision is None:
                await message.reply_text("Iltimos 'ha' yoki 'yo'q' deb yozing.")
                return True
            draft["is_return"] = decision
            draft["stage"] = "dn_source_wh"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_delivery_source_warehouse(chat_id=chat_id, context=context)
            return True

        if stage == "dn_source_wh":
            data = self._parse_warehouse_inline(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Inline oynadan omborni tanlang.")
                return True
            draft["source_warehouse"] = data.get("code")
            draft["stage"] = "dn_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(f"{data.get('label')} tanlandi.")
            await self._prompt_delivery_items_menu(chat_id=chat_id, draft=draft, context=context)
            return True

        if stage == "dn_items_menu":
            data = self._parse_delivery_item(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Buyum tanlash uchun inline tugmasidan foydalaning.")
                return True
            draft["current_item"] = data
            draft["stage"] = "dn_item_qty"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                f"{data.get('name')} uchun miqdorni kiriting.",
                reply_markup=self._delivery_cancel_markup(),
            )
            return True

        if stage == "dn_item_qty":
            normalized_qty = text.replace(",", ".")
            try:
                qty = float(normalized_qty)
            except ValueError:
                await message.reply_text("Miqdor noto'g'ri. Masalan: 25")
                return True
            if qty <= 0:
                await message.reply_text("Miqdor musbat bo'lishi kerak.")
                return True
            current_item = draft.get("current_item") or {}
            current_item["qty"] = qty
            draft["current_item"] = current_item
            draft["stage"] = "dn_item_rate"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                "Narxni kiriting (masalan: 12000). Kerak bo'lmasa 0 deb yozing yoki 'skip'.",
                reply_markup=self._delivery_cancel_markup(),
            )
            return True

        if stage == "dn_item_rate":
            normalized_rate = text.replace(",", ".")
            if normalized in skip_values:
                rate = 0.0
            else:
                try:
                    rate = float(normalized_rate)
                except ValueError:
                    await message.reply_text("Narx noto'g'ri. Masalan: 12000")
                    return True
                if rate < 0:
                    await message.reply_text("Narx manfiy bo'lmasligi kerak.")
                    return True
            current_item = draft.get("current_item") or {}
            if not current_item:
                return True
            qty = float(current_item.get("qty") or 0)
            items = draft.get("items") or []
            item_entry = {
                "code": current_item.get("code"),
                "name": current_item.get("name"),
                "uom": current_item.get("uom"),
                "qty": qty,
                "rate": rate,
                "amount": rate * qty,
            }
            items.append(item_entry)
            draft["items"] = items
            draft["current_item"] = None
            draft["stage"] = "dn_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                f"{item_entry.get('name')} qo'shildi.",
                reply_markup=self._delivery_cancel_markup(),
            )
            await self._prompt_delivery_items_menu(chat_id=chat_id, draft=draft, context=context)
            return True

        return False

    async def _finalise_delivery_note_creation(
        self,
        *,
        user_id: int,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        customer = draft.get("customer")
        items = draft.get("items") or []
        source_warehouse = draft.get("source_warehouse")
        chat_id = draft.get("chat_id", user_id)
        if not customer or not source_warehouse or not items:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Ma'lumotlar yetarli emas. Mijoz, ombor va kamida bitta buyumni tanlang.",
                reply_markup=self._delivery_cancel_markup(),
            )
            draft["stage"] = "dn_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            return
        payload_items = []
        for row in items:
            payload_items.append(
                {
                    "item_code": row.get("code"),
                    "item_name": row.get("name"),
                    "qty": row.get("qty"),
                    "uom": row.get("uom"),
                    "rate": row.get("rate"),
                    "amount": row.get("amount"),
                    "warehouse": source_warehouse,
                }
            )
        payload = {
            "customer": customer.get("code"),
            "posting_date": draft.get("posting_date"),
            "posting_time": draft.get("posting_time"),
            "company": self.config.default_company,
            "set_warehouse": source_warehouse,
            "is_return": 1 if draft.get("is_return") else 0,
            "items": payload_items,
            "naming_series": self.config.delivery_note_series_template,
        }
        success, error_detail, docname = await self._create_delivery_note(
            api_key,
            api_secret,
            payload=payload,
        )
        if success:
            self.storage.delete_entry_draft(user_id)
            detail_success = False
            detail: Dict[str, Any] = {}
            if docname:
                detail_success, _, detail = await self._fetch_delivery_note_detail(
                    api_key,
                    api_secret,
                    docname,
                )
            if detail_success:
                summary = detail.copy()
                text_message = self._format_delivery_note_message(summary, detail)
                markup = self._delivery_action_buttons(detail)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text_message,
                    reply_markup=markup,
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "✅ Chiqqan mahsulotni rasmiylashtirish yakunlandi.\n"
                        f"Nom: {docname or 'ERPNext'}\n"
                        f"Mijoz: {customer.get('label')}\n"
                        f"Ombor: {source_warehouse}\n"
                        f"Buyumlar soni: {len(items)}"
                    ),
                )
        else:
            draft["stage"] = "dn_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            message = error_detail or "Chiqqan mahsulot hujjatini yaratishda xatolik yuz berdi."
            await context.bot.send_message(
                chat_id=chat_id,
                text=message + "\nJarayonni davom ettirish yoki bekor qilish mumkin.",
                reply_markup=self._delivery_cancel_markup(),
            )

    async def _handle_delivery_approve_message(
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
        token_line = next((line for line in lines if line.startswith(f"{DELIVERY_APPROVE_PREFIX}:")), None)
        if not token_line:
            return False
        _, docname = token_line.split(":", 1)
        docname = docname.strip()
        if not docname:
            return False
        success, error_detail, detail = await self._fetch_delivery_note_detail(api_key, api_secret, docname)
        if not success:
            fallback = error_detail or "Ma'lumot topilmadi."
            await message.reply_text(f"Tasdiqlashda xatolik:\n{fallback}")
            return True
        summary = detail.copy()
        text_message = self._format_delivery_note_message(summary, detail)
        markup = self._delivery_action_buttons(detail)
        await message.reply_text(text_message, reply_markup=markup)
        self.storage.delete_entry_draft(user_id)
        return True

    # -------------------------------------------------------------- handlers
    async def handle_delivery_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        await self._send_delivery_preview(
            chat_id=chat.id,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
        )
        await message.reply_text(
            "Chiqqan mahsulotlarni rasmiylashtirish menyusi:",
            reply_markup=self._delivery_markup(),
        )

    async def handle_delivery_create_callback(
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
        await context.bot.send_message(chat_id=chat_id, text="Yangi chiqim hujjatini yaratishni boshlaymiz.")
        await self._start_delivery_note_creation(user_id=user.id, chat_id=chat_id, context=context)

    async def handle_delivery_creation_callback(
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
        if not draft or draft.get("kind") != "delivery_note":
            await query.answer("Chiqqan mahsulot hujjati jarayoni topilmadi.", show_alert=True)
            return
        chat_id = draft.get("chat_id") or (query.message.chat_id if query.message else user.id)
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""

        if action == "finish":
            if not draft.get("customer"):
                await query.answer("Avval mijoz tanlang.", show_alert=True)
                return
            if not draft.get("source_warehouse"):
                await query.answer("Avval omborni tanlang.", show_alert=True)
                return
            if not draft.get("items"):
                await query.answer("Hech bo'lmaganda bitta buyum qo'shing.", show_alert=True)
                return
            draft["stage"] = "dn_submitting"
            self.storage.save_entry_draft(user.id, draft)
            await query.answer("Yaratilmoqda…", show_alert=False)
            await context.bot.send_message(chat_id=chat_id, text="⏳ Chiqqan mahsulot hujjati yaratilmoqda...")
            await self._finalise_delivery_note_creation(
                user_id=user.id,
                draft=draft,
                api_key=api_key,
                api_secret=api_secret,
                context=context,
            )
            return

        if action == "cancel":
            await query.answer("Jarayon bekor qilindi.", show_alert=False)
            await self._cancel_entry_creation(
                user_id=user.id,
                chat_id=chat_id,
                context=context,
                notice="Chiqqan mahsulot hujjati jarayoni bekor qilindi.",
            )
            return

        if action == "yn":
            stage = draft.get("stage")
            decision = value == "yes"
            if stage == "dn_is_return":
                draft["is_return"] = decision
                draft["stage"] = "dn_source_wh"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("Tanlandi.", show_alert=False)
                await self._prompt_delivery_source_warehouse(chat_id=chat_id, context=context)
                return
            await query.answer("Bu bosqichda Ha/Yo'q tugmalari aktyor emas.", show_alert=True)
            return

        if action == "skip":
            stage = draft.get("stage")
            if stage == "dn_date":
                draft["stage"] = "dn_time"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_delivery_posting_time(
                    chat_id=chat_id,
                    current_time=draft.get("posting_time") or "",
                    context=context,
                )
                return
            if stage == "dn_time":
                draft["stage"] = "dn_is_return"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_delivery_return_choice(chat_id=chat_id, context=context)
                return
            if stage == "dn_item_rate":
                current_item = draft.get("current_item") or {}
                if not current_item:
                    await query.answer("Buyum topilmadi.", show_alert=True)
                    return
                qty = float(current_item.get("qty") or 0)
                items = draft.get("items") or []
                items.append(
                    {
                        "code": current_item.get("code"),
                        "name": current_item.get("name"),
                        "uom": current_item.get("uom"),
                        "qty": qty,
                        "rate": 0.0,
                        "amount": 0.0,
                    }
                )
                draft["items"] = items
                draft["current_item"] = None
                draft["stage"] = "dn_items_menu"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("0 narx bilan qo'shildi.", show_alert=False)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Buyum qo'shildi.",
                    reply_markup=self._delivery_cancel_markup(),
                )
                await self._prompt_delivery_items_menu(chat_id=chat_id, draft=draft, context=context)
                return
            await query.answer("Bu bosqichda o'tkazib yuborish tugmasi mavjud emas.", show_alert=True)
            return

        await query.answer("Noma'lum tanlov.", show_alert=True)

    async def handle_delivery_confirm_callback(
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
        chat_id = query.message.chat_id if query.message else user.id
        draft = {
            "kind": "delivery_confirm",
            "stage": "await_delivery_confirm",
            "chat_id": chat_id,
        }
        self.storage.save_entry_draft(user.id, draft)
        await query.answer("Inline oynani oching.", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="Tasdiqlash yoki bekor qilish uchun chiqqan mahsulot hujjatini qidiring.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🚚 Tasdiqlash oynasini ochish",
                            switch_inline_query_current_chat=f"{DELIVERY_APPROVE_QUERY_PREFIXES[0]} ",
                        )
                    ]
                ]
            ),
        )

    async def handle_delivery_approve_action(
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
            await query.answer("Chiqqan mahsulot hujjati aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._submit_delivery_note(
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

    async def handle_delivery_cancel_action(
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
            await query.answer("Chiqqan mahsulot hujjati aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._cancel_delivery_note(
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
        else:
            await query.answer("Xatolik yuz berdi.", show_alert=True)
            fallback = error_detail or "Noma'lum"
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"Bekor qilishda xatolik:\n{fallback}",
            )

    async def handle_delivery_delete_action(
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
            await query.answer("Chiqqan mahsulot hujjati aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._delete_delivery_note(
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
        else:
            await query.answer("Xatolik yuz berdi.", show_alert=True)
            fallback = error_detail or "Noma'lum"
            await context.bot.send_message(
                chat_id=query.message.chat_id if query.message else user.id,
                text=f"O'chirishda xatolik:\n{fallback}",
            )

    async def handle_delivery_dismiss_action(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        docname = (query.data or "").split(":", 1)
        docname = docname[1] if len(docname) == 2 else ""
        await query.answer("Saqlab qo'yildi.", show_alert=False)
        if query.message:
            await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else query.from_user.id,
            text=f"📁 {docname or 'Chiqqan mahsulot hujjati'} draft holatida saqlanmoqda.",
        )

    # -------------------------------------------------------------- previews
    async def _send_delivery_preview(
        self,
        *,
        chat_id: int,
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
        show_message: bool = True,
    ) -> None:
        if not show_message:
            return
        success, error_detail, rows = await self._fetch_delivery_notes(
            api_key,
            api_secret,
            query="",
        )
        if not success:
            text = "Chiqqan mahsulot hujjatlari ro'yxatini olishda xatolik yuz berdi."
            if error_detail:
                text += f"\nMa'lumot: {error_detail}"
            await context.bot.send_message(chat_id=chat_id, text=text)
            return
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="Hozircha chiqqan mahsulot hujjati topilmadi.")
            return
        preview = rows[:5]
        lines = []
        for row in preview:
            name = row.get("name") or "-"
            customer = row.get("customer") or "-"
            posting_date = row.get("posting_date") or "-"
            posting_time = row.get("posting_time") or "-"
            status = self._docstatus_label(row.get("docstatus"))
            lines.append(f"• {name} — {customer} ({posting_date} {posting_time}) — {status}")
        if len(rows) > len(preview):
            lines.append(f"... yana {len(rows) - len(preview)} ta chiqqan mahsulot hujjati inline menyuda mavjud.")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))

    # -------------------------------------------------------------- ERP calls
    async def _fetch_delivery_notes(
        self,
        api_key: str,
        api_secret: str,
        *,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/{quote('Delivery Note', safe='')}"
        params = {
            "fields": json.dumps(
                [
                    "name",
                    "customer",
                    "posting_date",
                    "posting_time",
                    "set_warehouse",
                    "grand_total",
                    "docstatus",
                ]
            ),
            "limit_page_length": str(self.config.delivery_note_limit),
            "order_by": "posting_date desc, posting_time desc",
        }
        if query:
            params["filters"] = json.dumps([["Delivery Note", "name", "like", f"%{query}%"]])

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
                return False, "Chiqqan mahsulot hujjatlari javobini o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Chiqqan mahsulot hujjatlari ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_customers(
        self,
        api_key: str,
        api_secret: str,
        *,
        limit: int = 25,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Customer"
        params = {
            "fields": json.dumps(["name", "customer_name", "customer_group"]),
            "limit_page_length": str(limit),
            "order_by": "customer_name asc",
        }
        query = (query or "").strip()
        if query:
            params["filters"] = json.dumps([["Customer", "customer_name", "like", f"%{query}%"]])

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
                return False, "Customer javobini o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Customer ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_delivery_note_detail(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Delivery Note/{quote(docname, safe='')}"

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
                return False, "Chiqqan mahsulot hujjati ma'lumotini o'qib bo'lmadi.", {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, dict):
                return False, "Chiqqan mahsulot hujjati ma'lumotlari topilmadi.", {}
            return True, None, data

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Chiqqan mahsulot hujjati tafsilotlarini olishda xatolik: %s", exc)
            return False, str(exc), {}

    async def _create_delivery_note(
        self,
        api_key: str,
        api_secret: str,
        *,
        payload: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Delivery Note"

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
            logger.warning("Delivery Note yaratishda xatolik: %s", exc)
            return False, str(exc), None

    async def _submit_delivery_note(
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
            payload = {"dt": "Delivery Note", "dn": docname, "method": "submit"}
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
            logger.warning("Delivery Note submitda xatolik: %s", exc)
            return False, str(exc)

    async def _cancel_delivery_note(
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
            payload = {"dt": "Delivery Note", "dn": docname, "method": "cancel"}
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
            logger.warning("Delivery Note cancelda xatolik: %s", exc)
            return False, str(exc)

    async def _delete_delivery_note(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Delivery Note/{quote(docname, safe='')}"

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
            logger.warning("Delivery Note ni o'chirishda xatolik: %s", exc)
            return False, str(exc)

    # ------------------------------------------------------------ formatters
    def _format_delivery_note_message(
        self,
        summary: Dict[str, Any],
        detail: Optional[Dict[str, Any]],
    ) -> str:
        detail = detail or {}
        name = summary.get("name") or detail.get("name") or "-"
        customer = detail.get("customer_name") or summary.get("customer") or detail.get("customer") or "-"
        posting_date = summary.get("posting_date") or detail.get("posting_date") or "-"
        posting_time = summary.get("posting_time") or detail.get("posting_time") or "-"
        warehouse = (
            summary.get("set_warehouse")
            or detail.get("set_warehouse")
            or (detail.get("items")[0].get("warehouse") if detail.get("items") else None)
            or "-"
        )
        total = detail.get("grand_total") or summary.get("grand_total") or "-"
        lines = [
            f"🚚 Chiqqan mahsulot hujjati: {name}",
            f"Mijoz: {customer}",
            f"Sana: {posting_date} {posting_time}",
            f"Ombor: {warehouse}",
            f"Jami: {total}",
            f"Status: {self._docstatus_label(detail.get('docstatus'))}",
        ]
        items = detail.get("items")
        if isinstance(items, list) and items:
            lines.append("")
            max_items = 10
            for item in items[:max_items]:
                lines.append(
                    f"• {item.get('item_code')} {item.get('item_name') or ''} — {item.get('qty')} {item.get('uom')} (Narx: {item.get('rate')})"
                )
            if len(items) > max_items:
                lines.append(f"... va yana {len(items) - max_items} ta buyum")
        return "\n".join(lines)

    def _delivery_action_buttons(self, detail: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
        docname = detail.get("name")
        if not docname:
            return None
        docstatus = detail.get("docstatus")
        buttons: list[list[InlineKeyboardButton]] = []
        if docstatus == 0:
            buttons.append(
                [
                    InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"{DELIVERY_APPROVE_PREFIX}:{docname}"),
                    InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{DELIVERY_DELETE_PREFIX}:{docname}"),
                ]
            )
            buttons.append(
                [
                    InlineKeyboardButton(
                        "📁 Saqlab chiqish", callback_data=f"{DELIVERY_DISMISS_PREFIX}:{docname}"
                    )
                ]
            )
        elif docstatus == 1:
            buttons.append(
                [InlineKeyboardButton("❌ Bekor qilish", callback_data=f"{DELIVERY_CANCEL_PREFIX}:{docname}")]
            )
        else:
            buttons.append(
                [InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{DELIVERY_DELETE_PREFIX}:{docname}")]
            )
        return InlineKeyboardMarkup(buttons)
