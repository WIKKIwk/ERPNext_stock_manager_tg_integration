# Stock Manager Bot

Mustaqil Telegram bot bo'lib, omborchi (stock manager) ERPNext bilan integratsiya orqali quyidagi ishlarni bajarishi mumkin.

## Asosiy imkoniyatlar

- `/start` buyrug'i bilan 14–18 belgilik API key/secret juftligini tasdiqlash va menyuni ochish.
- “📦 Buyumlarni ko'rish” tugmasi inline qidiruvni ishga tushirib, `Item` ro'yxatini filtr bilan ko'rsatadi.
- `/entry` menyusi orqali mavjud Stock Entry larni ko'rish yoki “Material kiridi/chiqdi” oqimi orqali yangisini yaratish.
- Purchase Receipt va Delivery Note larni ko'rish, tasdiqlash, bekor qilish hamda bot ichida yaratish.
- `/apic` buyrug'i bilan mavjud API kalitlarini, `/cancel` bilan esa davom etayotgan jarayonlarni tozalash.

## Talablar

- Docker Engine 24+ va `docker compose` (v2) plugin (`make` orqali ishlatish uchun).
- GNU Make.
- Lokal ishga tushirish uchun Python 3.10+, `pip` hamda Telegram bot tokeni va ERPNext API ruxsatlari.

## Tez start (`make` + Docker Compose)

1. Reponi yuklab oling va katalogga kiring.
2. `cp .env.example .env` buyrug'i bilan `.env` yarating va kamida `STOCK_BOT_TOKEN` hamda `FRAPPE_BASE_URL` qiymatlarini kiriting.
3. `make` buyrug'ini bering – u `docker compose up --build` ni chaqiradi va botni ishga tushiradi.

Foydali buyruqlar:

```bash
make           # build + up
make logs      # konteyner loglari
make down      # servislarni to'xtatish
```

`./data` katalogi konteynerdagi `/app/data` ga bind-mount qilingan bo'lib, SQLite bazasi shu yerda saqlanadi (repolarda u e'tiborga olinmaydi).

## Lokal ishga tushirish (Python)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env  # zarur sozlamalarni kiriting
python -m stock_manager_bot
```

Kodni bevosita ishga tushirganda `python-dotenv` avtomatik ravishda `.env` faylini o'qib beradi, shuning uchun alohida `export` qilish shart emas.

## Muhit o'zgaruvchilari

Minimal konfiguratsiya (`.env.example` asosida):

```
STOCK_BOT_TOKEN=xxx
FRAPPE_BASE_URL=https://erp.example.com
ERP_COMPANY=accord
STOCK_ENTRY_SERIES=MAT-STE-.YYYY.-.#####
```

Qo'shimcha parametrlar:

- `STOCK_BOT_DB_PATH` – SQLite faylining yo'li (default: `stock_manager_bot.sqlite3`).
- `ERP_VERIFY_ENDPOINT` – API kalitni tekshiruvchi endpoint (default: `/api/method/frappe.auth.get_logged_user`).
- `ITEM_LIMIT`, `WAREHOUSE_LIMIT`, `SUPPLIER_LIMIT`, `PURCHASE_RECEIPT_LIMIT`, `CUSTOMER_LIMIT`, `DELIVERY_NOTE_LIMIT` – tegishli inline qidiruv limitlari.
- `PURCHASE_RECEIPT_SERIES`, `DELIVERY_NOTE_SERIES` – tegishli seriyalar shabloni.

## Loyiha tuzilmasi

```
.
├── docker-compose.yml  # Docker orkestratsiyasi (make shu faylni chaqiradi)
├── Dockerfile          # Runtime image
├── Makefile            # make, make logs, make down
├── pyproject.toml      # Python metadata va bog'liqliklar
├── README.md           # Ushbu hujjat
└── stock_manager_bot/  # Bot logikasi (bot.py, purchase/delivery oqimlari, config va storage)
```

Savollar bo'lsa yoki qo'shimcha oqimlar kerak bo'lsa, issue ochish kifoya.
