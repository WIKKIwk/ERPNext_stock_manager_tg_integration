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
from telegram.ext import ContextTypes
from telegram.constants import ChatType

logger = logging.getLogger(__name__)

PURCHASE_TRIGGER = "purchase"
PURCHASE_CALLBACK_CREATE = "purchase:create"
PURCHASE_CREATE_PREFIX = "purchasecreate"
PURCHASE_SUPPLIER_QUERY_PREFIX = "supplier"
PURCHASE_ITEM_QUERY_PREFIX = "pritem"
PURCHASE_APPROVE_PREFIX = "purchase-approve"
PURCHASE_CANCEL_PREFIX = "purchase-cancel"
PURCHASE_DELETE_PREFIX = "purchase-delete"
PURCHASE_CONFIRM_CALLBACK = "purchase:confirm"
PURCHASE_APPROVE_QUERY_PREFIXES = ("purchaseapprove", "prapprove")


class PurchaseFlowMixin:
    """Encapsulates Purchase Receipt workflows."""

    # ------------------------------------------------------------------ menus
    def _purchase_markup(self) -> InlineKeyboardMarkup:
        view_button = InlineKeyboardButton(
            "🧾 Purchase Receiptlarni ko'rish",
            switch_inline_query_current_chat=PURCHASE_TRIGGER,
        )
        create_button = InlineKeyboardButton(
            "➕ Yangi Purchase Receipt", callback_data=PURCHASE_CALLBACK_CREATE
        )
        confirm_button = InlineKeyboardButton(
            "✔️ Purchase Receiptni tasdiqlash", callback_data=PURCHASE_CONFIRM_CALLBACK
        )
        return InlineKeyboardMarkup([[view_button], [create_button], [confirm_button]])

    def _purchase_cancel_button(self) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            "❌ Jarayonni bekor qilish", callback_data=f"{PURCHASE_CREATE_PREFIX}:cancel"
        )

    def _purchase_cancel_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[self._purchase_cancel_button()]])

    def _skip_inline_button(self) -> InlineKeyboardButton:
        return InlineKeyboardButton("⏭ Skip", callback_data=f"{PURCHASE_CREATE_PREFIX}:skip")

    def _skip_inline_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [self._skip_inline_button()],
                [self._purchase_cancel_button()],
            ]
        )

    def _yes_no_inline_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Ha", callback_data=f"{PURCHASE_CREATE_PREFIX}:yn:yes"),
                    InlineKeyboardButton("Yo'q", callback_data=f"{PURCHASE_CREATE_PREFIX}:yn:no"),
                ],
                [self._purchase_cancel_button()],
            ]
        )

    # ------------------------------------------------------------ prompts/flow
    async def _start_purchase_receipt_creation(
        self,
        *,
        user_id: int,
        chat_id: int,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        now = datetime.utcnow()
        draft = {
            "kind": "purchase_receipt",
            "stage": "pr_supplier",
            "chat_id": chat_id,
            "series": self.config.purchase_receipt_series_template,
            "posting_date": now.strftime("%Y-%m-%d"),
            "posting_time": now.strftime("%H:%M"),
            "supplier_delivery_note": "",
            "apply_putaway_rule": False,
            "is_return": False,
            "items": [],
        }
        self.storage.save_entry_draft(user_id, draft)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🧾 Yangi Purchase Receipt seriyasi: {draft['series']}\n"
                f"Chop etish sanasi: {draft['posting_date']}, vaqt: {draft['posting_time']}.\n"
                "Iltimos, yetkazib beruvchini tanlang."
            ),
        )
        await self._prompt_purchase_supplier(chat_id=chat_id, context=context)

    async def _prompt_purchase_supplier(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👤 Yetkazib beruvchi oynasi",
                        switch_inline_query_current_chat=f"{PURCHASE_SUPPLIER_QUERY_PREFIX} ",
                    )
                ],
                [self._purchase_cancel_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Yetkazib beruvchini tanlang. Inline oynadan qidirib tanlang.",
            reply_markup=buttons,
        )

    async def _prompt_purchase_delivery_note(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Supplier Delivery Note ni kiriting (ixtiyoriy). 'Skip' tugmasini bosib o'tkazib yuborishingiz mumkin.",
            reply_markup=self._skip_inline_markup(),
        )

    async def _prompt_purchase_posting_date(
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
                "Agar o'zgartirmoqchi bo'lsangiz YYYY-MM-DD formatida yuboring yoki 'Skip' tugmasini bosing."
            ),
            reply_markup=self._skip_inline_markup(),
        )

    async def _prompt_purchase_posting_time(
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
                "HH:MM formatida vaqt yuboring yoki 'Skip' tugmasini bosing."
            ),
            reply_markup=self._skip_inline_markup(),
        )

    async def _prompt_purchase_putaway_choice(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Apply Putaway Rule qo'llansinmi?",
            reply_markup=self._yes_no_inline_markup(),
        )

    async def _prompt_purchase_return_choice(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Bu qaytariluvchi (Is Return) receiptmi?",
            reply_markup=self._yes_no_inline_markup(),
        )

    async def _prompt_purchase_accepted_warehouse(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🏬 Qabul qiluvchi ombor",
                        switch_inline_query_current_chat="warehouse ",
                    )
                ],
                [self._purchase_cancel_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Qabul qilingan tovarlar qaysi omborga keladi?",
            reply_markup=markup,
        )

    async def _prompt_purchase_rejected_warehouse(
        self, *, chat_id: int, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🏬 Qaytarilgan omborni tanlash",
                        switch_inline_query_current_chat="warehouse ",
                    )
                ],
                [self._skip_inline_button()],
                [self._purchase_cancel_button()],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text="Rejected Warehouse kerak bo'lsa tanlang, aks holda 'Skip' tugmasini bosing.",
            reply_markup=markup,
        )

    def _purchase_items_markup(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📦 Item qidirish",
                        switch_inline_query_current_chat=f"{PURCHASE_ITEM_QUERY_PREFIX} ",
                    )
                ],
                [InlineKeyboardButton("✅ Receiptni yaratish", callback_data=f"{PURCHASE_CREATE_PREFIX}:finish")],
                [self._purchase_cancel_button()],
            ]
        )

    def _purchase_action_buttons(self, detail: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
        docname = detail.get("name")
        if not docname:
            return None
        docstatus = detail.get("docstatus")
        buttons: list[list[InlineKeyboardButton]] = []
        if docstatus == 0:
            buttons.append(
                [
                    InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"{PURCHASE_APPROVE_PREFIX}:{docname}"),
                    InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{PURCHASE_DELETE_PREFIX}:{docname}"),
                ]
            )
        elif docstatus == 1:
            buttons.append(
                [InlineKeyboardButton("❌ Bekor qilish", callback_data=f"{PURCHASE_CANCEL_PREFIX}:{docname}")]
            )
        else:
            buttons.append(
                [InlineKeyboardButton("🗑️ O'chirish", callback_data=f"{PURCHASE_DELETE_PREFIX}:{docname}")]
            )
        return InlineKeyboardMarkup(buttons)

    async def _prompt_purchase_items_menu(
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
                    f"{idx}. {item.get('name')} — Qabul: {item.get('accepted_qty')} ({item.get('uom')})"
                )
            summary = "\n".join(lines)
        else:
            summary = "Hozircha item qo'shilmagan."
        await context.bot.send_message(
            chat_id=chat_id,
            text=summary + "\nYangi item qo'shish yoki yakunlash uchun pastdagi tugmalardan foydalaning.",
            reply_markup=self._purchase_items_markup(),
        )

    # ---------------------------------------------------------------- parsing
    @staticmethod
    def _parse_supplier_inline(text: str) -> Optional[Dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines or not any("#supplier" in line.lower() or "supplier:" in line.lower() for line in lines):
            return None
        code = None
        label = None
        for line in lines:
            lowered = line.lower()
            if lowered.startswith("supplier:"):
                label = line.split(":", 1)[1].strip()
            if lowered.startswith("code:"):
                code = line.split(":", 1)[1].strip()
        if not code:
            code = label
        if not code:
            return None
        return {"code": code, "label": label or code}

    @staticmethod
    def _parse_pr_item_inline(text: str) -> Optional[Dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines or not any("#pritem" in line.lower() for line in lines):
            return None
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
            return None
        return {"code": code, "name": name or code, "uom": uom or "-"}

    @staticmethod
    def _parse_yes_no(value: str) -> Optional[bool]:
        normalized = (value or "").strip().lower()
        positives = {"ha", "ha.", "yes", "y", "true", "1"}
        negatives = {"yo'q", "yoq", "yo'q.", "no", "n", "false", "0"}
        if normalized in positives:
            return True
        if normalized in negatives:
            return False
        return None

    # ----------------------------------------------------------- flow handler
    async def _handle_purchase_receipt_message(
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
        skip_values = {"skip", "-", "yo'q", "yoq", "otkaz", "o'tkaz"}

        if stage == "pr_supplier":
            data = self._parse_supplier_inline(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Inline oynadan yetkazib beruvchini tanlang.")
                return True
            draft["supplier"] = data
            draft["stage"] = "pr_supplier_note"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(f"{data.get('label')} tanlandi.")
            await self._prompt_purchase_delivery_note(chat_id=chat_id, context=context)
            return True

        if stage == "pr_supplier_note":
            draft["supplier_delivery_note"] = "" if normalized in skip_values else text
            draft["stage"] = "pr_date"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_posting_date(
                chat_id=chat_id,
                current_date=draft.get("posting_date") or "",
                context=context,
            )
            return True

        if stage == "pr_date":
            if normalized not in skip_values:
                try:
                    parsed = datetime.strptime(text.strip(), "%Y-%m-%d")
                    draft["posting_date"] = parsed.strftime("%Y-%m-%d")
                except ValueError:
                    await message.reply_text("Sana formatini YYYY-MM-DD ko'rinishida yuboring.")
                    return True
            draft["stage"] = "pr_time"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_posting_time(
                chat_id=chat_id,
                current_time=draft.get("posting_time") or "",
                context=context,
            )
            return True

        if stage == "pr_time":
            if normalized not in skip_values:
                try:
                    parsed = datetime.strptime(text.strip(), "%H:%M")
                    draft["posting_time"] = parsed.strftime("%H:%M")
                except ValueError:
                    await message.reply_text("Vaqt formatini HH:MM ko'rinishida yuboring.")
                    return True
            draft["stage"] = "pr_putaway"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_putaway_choice(chat_id=chat_id, context=context)
            return True

        if stage == "pr_putaway":
            decision = self._parse_yes_no(text)
            if decision is None:
                await message.reply_text("Iltimos 'ha' yoki 'yo'q' deb yozing.")
                return True
            draft["apply_putaway_rule"] = decision
            draft["stage"] = "pr_is_return"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_return_choice(chat_id=chat_id, context=context)
            return True

        if stage == "pr_is_return":
            decision = self._parse_yes_no(text)
            if decision is None:
                await message.reply_text("Iltimos 'ha' yoki 'yo'q' deb yozing.")
                return True
            draft["is_return"] = decision
            draft["stage"] = "pr_accepted_wh"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_accepted_warehouse(chat_id=chat_id, context=context)
            return True

        if stage == "pr_accepted_wh":
            data = self._parse_warehouse_inline(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Inline oynadan ombor tanlang.")
                return True
            draft["accepted_warehouse"] = data.get("code")
            draft["stage"] = "pr_rejected_wh"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(f"{data.get('label')} qabul qiluvchi ombor sifatida tanlandi.")
            await self._prompt_purchase_rejected_warehouse(chat_id=chat_id, context=context)
            return True

        if stage == "pr_rejected_wh":
            if normalized in skip_values and not from_inline_result:
                draft["rejected_warehouse"] = None
            else:
                data = self._parse_warehouse_inline(text)
                if data:
                    draft["rejected_warehouse"] = data.get("code")
                    await message.reply_text(f"{data.get('label')} rejected ombor sifatida tanlandi.")
                elif from_inline_result:
                    return True
                elif normalized not in skip_values:
                    await message.reply_text("Inline oynadan ombor tanlang yoki 'skip' deb yozing.")
                    return True
                else:
                    draft["rejected_warehouse"] = None
            draft["stage"] = "pr_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            await self._prompt_purchase_items_menu(chat_id=chat_id, draft=draft, context=context)
            return True

        if stage == "pr_items_menu":
            data = self._parse_pr_item_inline(text)
            if not data:
                if not from_inline_result:
                    await message.reply_text("Item tanlash uchun inline tugmasidan foydalaning.")
                return True
            draft["current_item"] = data
            draft["stage"] = "pr_item_qty"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                f"{data.get('name')} uchun qabul qilingan miqdorni kiriting.",
                reply_markup=self._purchase_cancel_markup(),
            )
            return True

        if stage == "pr_item_qty":
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
            current_item["accepted_qty"] = qty
            draft["current_item"] = current_item
            draft["stage"] = "pr_item_rejected_qty"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                "Rejected Quantity ni kiriting. Kerak bo'lmasa 'Skip' tugmasini bosing.",
                reply_markup=self._skip_inline_markup(),
            )
            return True

        if stage == "pr_item_rejected_qty":
            if normalized in skip_values:
                rejected = 0.0
            else:
                normalized_qty = text.replace(",", ".")
                try:
                    rejected = float(normalized_qty)
                except ValueError:
                    await message.reply_text("Miqdor noto'g'ri. Masalan: 0 yoki 1.5")
                    return True
                if rejected < 0:
                    await message.reply_text("Miqdor manfiy bo'lmasligi kerak.")
                    return True
            current_item = draft.get("current_item") or {}
            current_item["rejected_qty"] = rejected
            draft["current_item"] = current_item
            draft["stage"] = "pr_item_rate"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                "Narxni kiriting (masalan: 12000). Agar kerak bo'lmasa 0 deb yozing.",
                reply_markup=self._purchase_cancel_markup(),
            )
            return True

        if stage == "pr_item_rate":
            normalized_rate = text.replace(",", ".")
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
            items = draft.get("items") or []
            item_entry = {
                "code": current_item.get("code"),
                "name": current_item.get("name"),
                "uom": current_item.get("uom"),
                "accepted_qty": current_item.get("accepted_qty"),
                "rejected_qty": current_item.get("rejected_qty", 0.0),
                "rate": rate,
                "amount": rate * float(current_item.get("accepted_qty") or 0),
            }
            items.append(item_entry)
            draft["items"] = items
            draft["current_item"] = None
            draft["stage"] = "pr_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            await message.reply_text(
                f"{item_entry.get('name')} qo'shildi.",
                reply_markup=self._purchase_cancel_markup(),
            )
            await self._prompt_purchase_items_menu(chat_id=chat_id, draft=draft, context=context)
            return True

        return False

    async def _finalise_purchase_receipt_creation(
        self,
        *,
        user_id: int,
        draft: Dict[str, Any],
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        supplier = draft.get("supplier")
        items = draft.get("items") or []
        accepted_warehouse = draft.get("accepted_warehouse")
        chat_id = draft.get("chat_id", user_id)
        if not supplier or not accepted_warehouse or not items:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Ma'lumotlar yetarli emas. Supplier, ombor va kamida 1 ta item tanlang.",
                reply_markup=self._purchase_cancel_markup(),
            )
            draft["stage"] = "pr_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            return
        payload_items = []
        for row in items:
            accepted_qty = float(row.get("accepted_qty") or 0)
            rejected_qty = float(row.get("rejected_qty") or 0)
            payload_item = {
                "item_code": row.get("code"),
                "item_name": row.get("name"),
                "qty": accepted_qty,
                "received_qty": accepted_qty + rejected_qty,
                "accepted_qty": accepted_qty,
                "rejected_qty": rejected_qty,
                "warehouse": accepted_warehouse,
                "uom": row.get("uom"),
                "rate": row.get("rate"),
                "amount": row.get("amount"),
            }
            rejected_wh = draft.get("rejected_warehouse")
            if rejected_wh:
                payload_item["rejected_warehouse"] = rejected_wh
            payload_items.append(payload_item)
        payload = {
            "supplier": supplier.get("code"),
            "posting_date": draft.get("posting_date"),
            "posting_time": draft.get("posting_time"),
            "supplier_delivery_note": draft.get("supplier_delivery_note"),
            "apply_putaway_rule": 1 if draft.get("apply_putaway_rule") else 0,
            "is_return": 1 if draft.get("is_return") else 0,
            "set_warehouse": accepted_warehouse,
            "company": self.config.default_company,
            "items": payload_items,
            "naming_series": self.config.purchase_receipt_series_template,
        }
        success, error_detail, docname = await self._create_purchase_receipt(
            api_key,
            api_secret,
            payload=payload,
        )
        if success:
            self.storage.delete_entry_draft(user_id)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "✅ Purchase Receipt yaratildi.\n"
                    f"Nom: {docname or 'ERPNext'}\n"
                    f"Supplier: {supplier.get('label')}\n"
                    f"Ombor: {accepted_warehouse}\n"
                    f"Itemlar soni: {len(items)}"
                ),
            )
        else:
            draft["stage"] = "pr_items_menu"
            self.storage.save_entry_draft(user_id, draft)
            message = error_detail or "Purchase Receipt yaratishda xatolik yuz berdi."
            await context.bot.send_message(
                chat_id=chat_id,
                text=message + "\nJarayonni davom ettirish yoki bekor qilish mumkin.",
                reply_markup=self._purchase_cancel_markup(),
            )

    async def _handle_purchase_approve_message(
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
        token_line = next((line for line in lines if line.startswith(f"{PURCHASE_APPROVE_PREFIX}:")), None)
        if not token_line:
            return False
        _, docname = token_line.split(":", 1)
        docname = docname.strip()
        if not docname:
            return False
        success, error_detail, detail = await self._fetch_purchase_receipt_detail(api_key, api_secret, docname)
        if not success:
            fallback = error_detail or "Ma'lumot topilmadi."
            await message.reply_text(f"Tasdiqlashda xatolik:\n{fallback}")
            return True
        summary = detail.copy()
        text_message = self._format_purchase_receipt_message(summary, detail)
        markup = self._purchase_action_buttons(detail)
        await message.reply_text(text_message, reply_markup=markup)
        self.storage.delete_entry_draft(user_id)
        return True

    # --------------------------------------------------------------- handlers
    async def handle_purchase_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        await self._send_purchase_preview(
            chat_id=chat.id,
            api_key=api_key,
            api_secret=api_secret,
            context=context,
        )
        await message.reply_text(
            "Purchase Receipt menyusi:",
            reply_markup=self._purchase_markup(),
        )

    async def handle_purchase_create_callback(
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
        await context.bot.send_message(chat_id=chat_id, text="Yangi Purchase Receipt yaratishni boshlaymiz.")
        await self._start_purchase_receipt_creation(user_id=user.id, chat_id=chat_id, context=context)

    async def handle_purchase_creation_callback(
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
            await query.answer("Purchase Receipt jarayoni topilmadi.", show_alert=True)
            return
        draft_kind = draft.get("kind") or "stock_entry"
        if draft_kind != "purchase_receipt":
            await query.answer("Purchase Receipt jarayoni topilmadi.", show_alert=True)
            return
        chat_id = draft.get("chat_id") or (query.message.chat_id if query.message else user.id)
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        api_key = creds.get("api_key") or ""
        api_secret = creds.get("api_secret") or ""

        if action == "finish":
            if draft.get("stage") not in {"pr_items_menu", "pr_item_rate"}:
                await query.answer("Jarayon hali to'liq emas.", show_alert=True)
                return
            if not draft.get("supplier"):
                await query.answer("Avval supplier tanlang.", show_alert=True)
                return
            if not draft.get("accepted_warehouse"):
                await query.answer("Avval qabul qiluvchi omborni tanlang.", show_alert=True)
                return
            if not draft.get("items"):
                await query.answer("Hech bo'lmaganda bitta item qo'shing.", show_alert=True)
                return
            draft["stage"] = "pr_submitting"
            self.storage.save_entry_draft(user.id, draft)
            await query.answer("Yaratilmoqda…", show_alert=False)
            await context.bot.send_message(chat_id=chat_id, text="⏳ Purchase Receipt yaratilmoqda...")
            await self._finalise_purchase_receipt_creation(
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
                notice="Purchase Receipt jarayoni bekor qilindi.",
            )
            return

        if action == "yn":
            decision = value == "yes"
            stage = draft.get("stage")
            if stage == "pr_putaway":
                draft["apply_putaway_rule"] = decision
                draft["stage"] = "pr_is_return"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("Tanlandi.", show_alert=False)
                await self._prompt_purchase_return_choice(chat_id=chat_id, context=context)
                return
            if stage == "pr_is_return":
                draft["is_return"] = decision
                draft["stage"] = "pr_accepted_wh"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("Tanlandi.", show_alert=False)
                await self._prompt_purchase_accepted_warehouse(chat_id=chat_id, context=context)
                return
            await query.answer("Bu bosqichda ha/yo'q tugmasi mavjud emas.", show_alert=True)
            return

        if action == "skip":
            stage = draft.get("stage")
            if stage == "pr_supplier_note":
                draft["supplier_delivery_note"] = ""
                draft["stage"] = "pr_date"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_purchase_posting_date(
                    chat_id=chat_id,
                    current_date=draft.get("posting_date") or "",
                    context=context,
                )
                return
            if stage == "pr_date":
                draft["stage"] = "pr_time"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_purchase_posting_time(
                    chat_id=chat_id,
                    current_time=draft.get("posting_time") or "",
                    context=context,
                )
                return
            if stage == "pr_time":
                draft["stage"] = "pr_putaway"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_purchase_putaway_choice(chat_id=chat_id, context=context)
                return
            if stage == "pr_rejected_wh":
                draft["rejected_warehouse"] = None
                draft["stage"] = "pr_items_menu"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await self._prompt_purchase_items_menu(chat_id=chat_id, draft=draft, context=context)
                return
            if stage == "pr_item_rejected_qty":
                current_item = draft.get("current_item") or {}
                current_item["rejected_qty"] = 0.0
                draft["current_item"] = current_item
                draft["stage"] = "pr_item_rate"
                self.storage.save_entry_draft(user.id, draft)
                await query.answer("O'tkazildi.", show_alert=False)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Narxni kiriting (masalan: 12000). Agar kerak bo'lmasa 0 deb yozing.",
                    reply_markup=self._purchase_cancel_markup(),
                )
                return
            await query.answer("Bu bosqichda Skip tugmasi mavjud emas.", show_alert=True)
            return

        await query.answer("Noma'lum tanlov.", show_alert=True)

    async def handle_purchase_confirm_callback(
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
            "kind": "purchase_confirm",
            "stage": "await_purchase_confirm",
            "chat_id": chat_id,
        }
        self.storage.save_entry_draft(user.id, draft)
        await query.answer("Inline oynani oching.", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="Tasdiqlash yoki bekor qilish uchun quyidagi oynani ochib Purchase Receiptni qidiring.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🧾 Tasdiqlash oynasini ochish",
                            switch_inline_query_current_chat=f"{PURCHASE_APPROVE_QUERY_PREFIXES[0]} ",
                        )
                    ]
                ]
            ),
        )

    async def handle_purchase_approve_action(
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
            await query.answer("Purchase Receipt aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._submit_purchase_receipt(
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

    async def handle_purchase_cancel_action(
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
            await query.answer("Purchase Receipt aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._cancel_purchase_receipt(
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

    async def handle_purchase_delete_action(
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
            await query.answer("Purchase Receipt aniqlanmadi.", show_alert=True)
            return
        creds = self.storage.get_credentials(user.id)
        if not creds or creds.get("status") != "active":
            await query.answer("Avval /start orqali API kalitlarini sozlang.", show_alert=True)
            return
        success, error_detail = await self._delete_purchase_receipt(
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

    # --------------------------------------------------------------- previews
    async def _send_purchase_preview(
        self,
        *,
        chat_id: int,
        api_key: str,
        api_secret: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        success, error_detail, rows = await self._fetch_purchase_receipts(
            api_key,
            api_secret,
            query="",
        )
        if not success:
            text = "Purchase Receipt ro'yxatini olishda xatolik yuz berdi."
            if error_detail:
                text += f"\nMa'lumot: {error_detail}"
            await context.bot.send_message(chat_id=chat_id, text=text)
            return
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="Hozircha Purchase Receipt topilmadi.")
            return
        preview = rows[:5]
        lines = []
        for row in preview:
            name = row.get("name") or "-"
            supplier = row.get("supplier") or "-"
            posting_date = row.get("posting_date") or "-"
            posting_time = row.get("posting_time") or "-"
            status = self._docstatus_label(row.get("docstatus"))
            lines.append(f"• {name} — {supplier} ({posting_date} {posting_time}) — {status}")
        if len(rows) > len(preview):
            lines.append(f"... yana {len(rows) - len(preview)} ta Purchase Receipt inline menyuda mavjud.")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))

    async def _fetch_purchase_receipts(
        self,
        api_key: str,
        api_secret: str,
        *,
        query: str = "",
    ) -> Tuple[bool, Optional[str], list[Dict[str, Any]]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/{quote('Purchase Receipt', safe='')}"
        params = {
            "fields": json.dumps(
                [
                    "name",
                    "supplier",
                    "posting_date",
                    "posting_time",
                    "set_warehouse",
                    "grand_total",
                    "docstatus",
                ]
            ),
            "limit_page_length": str(self.config.purchase_receipt_limit),
            "order_by": "posting_date desc, posting_time desc",
        }
        if query:
            params["filters"] = json.dumps([["Purchase Receipt", "name", "like", f"%{query}%"]])

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
                return False, "Purchase Receipt javobini o'qib bo'lmadi.", []
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []
            return True, None, data  # type: ignore[list-item]

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Purchase Receipt ro'yxatini olishda xatolik: %s", exc)
            return False, str(exc), []

    async def _fetch_purchase_receipt_detail(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Purchase Receipt/{quote(docname, safe='')}"

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
                return False, "Purchase Receipt ma'lumotini o'qib bo'lmadi.", {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, dict):
                return False, "Purchase Receipt ma'lumotlari topilmadi.", {}
            return True, None, data

        try:
            return await asyncio.to_thread(_request)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Purchase Receipt tafsilotlarini olishda xatolik: %s", exc)
            return False, str(exc), {}

    async def _create_purchase_receipt(
        self,
        api_key: str,
        api_secret: str,
        *,
        payload: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Purchase Receipt"

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
            logger.warning("Purchase Receipt yaratishda xatolik: %s", exc)
            return False, str(exc), None

    async def _submit_purchase_receipt(
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
            payload = {"dt": "Purchase Receipt", "dn": docname, "method": "submit"}
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
            logger.warning("Purchase Receipt submitda xatolik: %s", exc)
            return False, str(exc)

    async def _cancel_purchase_receipt(
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
            payload = {"dt": "Purchase Receipt", "dn": docname, "method": "cancel"}
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
            logger.warning("Purchase Receipt cancelda xatolik: %s", exc)
            return False, str(exc)

    async def _delete_purchase_receipt(
        self,
        api_key: str,
        api_secret: str,
        docname: str,
    ) -> Tuple[bool, Optional[str]]:
        endpoint = f"{self.config.frappe_base_url}/api/resource/Purchase Receipt/{quote(docname, safe='')}"

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
            logger.warning("Purchase Receipt ni o'chirishda xatolik: %s", exc)
            return False, str(exc)
