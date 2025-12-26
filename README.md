```
███████╗████████╗ ██████╗  ██████╗██╗  ██╗
██╔════╝╚══██╔══╝██╔═══██╗██╔════╝██║ ██╔╝
███████╗   ██║   ██║   ██║██║     █████╔╝ 
╚════██║   ██║   ██║   ██║██║     ██╔═██╗ 
███████║   ██║   ╚██████╔╝╚██████╗██║  ██╗
╚══════╝   ╚═╝    ╚═════╝  ╚═════╝╚═╝  ╚═╝
                                           
███╗   ███╗ █████╗ ███╗   ██╗ █████╗  ██████╗ ███████╗██████╗ 
████╗ ████║██╔══██╗████╗  ██║██╔══██╗██╔════╝ ██╔════╝██╔══██╗
██╔████╔██║███████║██╔██╗ ██║███████║██║  ███╗█████╗  ██████╔╝
██║╚██╔╝██║██╔══██║██║╚██╗██║██╔══██║██║   ██║██╔══╝  ██╔══██╗
██║ ╚═╝ ██║██║  ██║██║ ╚████║██║  ██║╚██████╔╝███████╗██║  ██║
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝
```

# TELEGRAM BOT :: ERPNEXT STOCK OPERATIONS INTERFACE

```
PROJECT: ERPNext Stock Manager Integration
RUNTIME: Python 3.10+ | Telegram Bot API
INTEGRATION: ERPNext v15.x via RESTful API
ARCHITECTURE: Standalone bot with SQLite persistence
LICENSE: MIT
```

---

## SYSTEM OVERVIEW

```
Self-contained Telegram bot providing mobile interface for ERPNext warehouse 
operations. Enables stock managers to perform inventory transactions, view 
items, manage stock entries, process purchase receipts, and handle delivery 
notes through Telegram messenger interface.

CRITICAL: ERPNext API credentials required for each user. Bot authenticates 
          individual users via their ERPNext API key/secret pairs.
```

---

## CAPABILITY MATRIX

```
CORE OPERATIONS
├── API Key Management
│   ├── User authentication via 14-18 character API key/secret
│   ├── Credential validation against ERPNext instance
│   └── Multi-user credential storage (encrypted SQLite)
│
├── Item Search & Lookup
│   ├── Inline search with autocomplete
│   ├── Item details retrieval (code, name, UOM)
│   └── Real-time ERPNext Item DocType queries
│
├── Stock Entry Workflow
│   ├── Material Receipt creation
│   ├── Material Issue processing
│   ├── Interactive item/warehouse selection
│   ├── Quantity input validation
│   ├── Draft/Submit/Cancel operations
│   └── Stock Entry approval workflow
│
├── Purchase Receipt Management
│   ├── View existing purchase receipts
│   ├── Create new purchase receipts via bot
│   ├── Supplier selection (inline query)
│   ├── Item addition with quantities
│   ├── Approval/cancellation workflows
│   └── Multi-step warehouse assignment
│
└── Delivery Note Processing
    ├── View existing delivery notes
    ├── Create delivery notes via bot
    ├── Customer selection (inline query)
    ├── Item addition with quantities
    ├── Approval/cancellation workflows
    └── Multi-step shipping operations

WORKFLOW ARCHITECTURE
┌────────────────────────────────────────────┐
│  Telegram User Interface                  │
│  (Mobile/Desktop Client)                  │
└────────────┬───────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────┐
│  Stock Manager Bot                        │
│  ├── Command Handlers                     │
│  ├── Inline Query Processors              │
│  ├── Callback Query Router                │
│  └── Conversation State Machine           │
└────────────┬───────────────────────────────┘
             │
      ┌──────┴──────┐
      ▼             ▼
┌─────────────┐  ┌──────────────┐
│ SQLite DB   │  │ ERPNext API  │
│ (Creds)     │  │ (REST)       │
└─────────────┘  └──────────────┘
```

---

## TECHNICAL REQUIREMENTS

```
RUNTIME ENVIRONMENT
├── Python: 3.10 or higher
├── Docker Engine: 24+ (for containerized deployment)
├── Docker Compose: v2+ plugin
└── GNU Make: 4.0+ (optional, for convenience commands)

PYTHON DEPENDENCIES
├── python-telegram-bot[rate-limiter]: >=21.2
├── requests: >=2.32
└── python-dotenv: >=1.0

EXTERNAL SERVICES
├── Telegram Bot API ........... Bot token from @BotFather
└── ERPNext Instance ........... v15.x with API access enabled

SYSTEM RESOURCES
├── RAM: 256MB minimum (512MB recommended)
├── Storage: 50MB application + database growth
└── Network: Persistent internet connection
```

---

## DEPLOYMENT PROTOCOLS

### [PROTOCOL 1] RAPID DEPLOYMENT (Docker Compose + Make)

```bash
# Step 1: Acquire source code
git clone https://github.com/WIKKIwk/ERPNext_stock_manager_tg_integration.git
cd ERPNext_stock_manager_tg_integration

# Step 2: Configure environment
cp .env.example .env
nano .env  # Configure STOCK_BOT_TOKEN, FRAPPE_BASE_URL, etc.

# Step 3: Deploy via Make
make           # Equivalent to: docker compose up --build

# Container logs (real-time)
make logs

# Shutdown
make down
```

**DEPLOYMENT SEQUENCE (Automated):**
1. Docker image build (Python 3.11-slim base)
2. Dependency installation (pip install .)
3. Container orchestration startup
4. SQLite database initialization
5. Telegram Bot API connection
6. Long polling activation

**DATA PERSISTENCE:**
- SQLite database located at `/app/data/stock_manager.sqlite3`
- Host mount point: `./data/` (automatically created)
- Credentials encrypted at rest using SQLite extensions

### [PROTOCOL 2] LOCAL DEVELOPMENT DEPLOYMENT

```bash
# Step 1: Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Step 2: Install package (editable mode)
pip install -e .

# Step 3: Environment configuration
cp .env.example .env
nano .env  # Required: STOCK_BOT_TOKEN, FRAPPE_BASE_URL

# Step 4: Launch bot
python -m stock_manager_bot

# Note: python-dotenv automatically loads .env file
# No manual export of environment variables required
```

---

## CONFIGURATION MATRIX

### MANDATORY ENVIRONMENT VARIABLES

```env
# Telegram Bot Authentication
STOCK_BOT_TOKEN=YOUR_BOT_TOKEN_FROM_BOTFATHER

# ERPNext Instance Configuration
FRAPPE_BASE_URL=https://your-erp-domain.com
# Note: No trailing slash

# Company Configuration
ERP_COMPANY=your_company_name
# Must match ERPNext Company DocType name exactly
```

### STOCK ENTRY CONFIGURATION

```env
# Stock Entry Naming Series Template
STOCK_ENTRY_SERIES=MAT-STE-.YYYY.-.#####
# Format follows ERPNext naming series convention
# Example result: MAT-STE-2025-00001
```

### PURCHASE RECEIPT CONFIGURATION

```env
# Purchase Receipt Naming Series (Optional)
PURCHASE_RECEIPT_SERIES=MAT-PRE-.YYYY.-.#####
# Default uses ERPNext system default if not specified
```

### DELIVERY NOTE CONFIGURATION

```env
# Delivery Note Naming Series (Optional)
DELIVERY_NOTE_SERIES=MAT-DN-.YYYY.-.#####
# Default uses ERPNext system default if not specified
```

### ADVANCED CONFIGURATION

```env
# API Verification Endpoint (Default: ERPNext standard)
ERP_VERIFY_ENDPOINT=/api/method/frappe.auth.get_logged_user
# Used to validate API credentials on bot /start

# SQLite Database Path
STOCK_BOT_DB_PATH=./stock_manager_bot.sqlite3
# Relative to working directory or absolute path

# Inline Query Result Limits
ITEM_LIMIT=25               # Item search results
WAREHOUSE_LIMIT=25          # Warehouse selection results
SUPPLIER_LIMIT=25           # Supplier search results
PURCHASE_RECEIPT_LIMIT=25   # Purchase receipt list
CUSTOMER_LIMIT=25           # Customer search results
DELIVERY_NOTE_LIMIT=25      # Delivery note list
```

---

## OPERATIONAL PROCEDURES

### INITIAL USER AUTHENTICATION

```
User Workflow:
[1] User sends /start to bot
[2] Bot requests ERPNext API credentials
[3] User provides 14-18 character API key
[4] User provides corresponding API secret
[5] Bot validates credentials against ERPNext
[6] If valid: credentials stored encrypted, main menu displayed
[7] If invalid: error message, re-prompt for credentials

API Key Requirements:
- Length: 14-18 alphanumeric characters
- Format: Obtained from ERPNext User Profile
- Permissions: Requires ERPNext API access enabled
- Validation: Real-time verification via ERPNext API call
```

### ITEM SEARCH INTERFACE

```
Access Method:
├── Command: /items
├── Keyboard Button: "📦 Buyumlar"
└── Inline Query: @bot_username items [search query]

Search Workflow:
[1] User initiates item search
[2] Bot presents inline query interface
[3] User types search query (item code or name)
[4] Bot queries ERPNext Item DocType
[5] Real-time results displayed (max: ITEM_LIMIT)
[6] User selects item from list
[7] Bot displays full item details
    ├── Item Code
    ├── Item Name
    ├── UOM (Unit of Measure)
    └── Additional metadata
```

### STOCK ENTRY CREATION WORKFLOW

```
Command: /entry

Stage 1: Entry Type Selection
├── Material Receipt (incoming goods)
└── Material Issue (outgoing goods)

Stage 2: Item Selection
├── Inline query activation
├── Search & select Item DocType
└── Item code captured

Stage 3: Warehouse Selection
├── Inline query activation
├── Search & select Warehouse DocType
├── Target warehouse (receipt) OR Source warehouse (issue)
└── Warehouse code captured

Stage 4: Quantity Input
├── Numeric input validation
├── Decimal support (e.g., 12.5)
└── Positive value enforcement

Stage 5: Submission
├── Stock Entry DocType creation via API
├── Automatic draft generation
├── Inline approval buttons
    ├── Submit (docstatus=1)
    ├── Cancel (docstatus=2)
    └── Delete (remove from system)

Entry States:
├── Draft (docstatus=0) .......... Editable, not in ledger
├── Submitted (docstatus=1) ...... Posted to ledger, immutable
└── Cancelled (docstatus=2) ...... Reversed in ledger, archived
```

### PURCHASE RECEIPT WORKFLOW

```
Command: /purchase

Viewing Existing Receipts:
[1] Bot queries Purchase Receipt DocType
[2] Results displayed with filters:
    ├── Supplier name
    ├── Posting date
    ├── Document status (Draft/Submitted/Cancelled)
    └── Total amount
[3] User selects receipt for details
[4] Action buttons provided:
    ├── Approve/Submit (if draft)
    ├── Cancel (if submitted)
    └── Delete (if draft)

Creating New Receipt:
[1] Bot initiates creation workflow
[2] Supplier selection (inline query)
[3] Item addition loop:
    ├── Select item (inline query)
    ├── Enter quantity
    ├── Enter rate (price)
    ├── Option: Add more items
[4] Warehouse assignment
[5] Review & confirm
[6] Submit to ERPNext API
[7] Success confirmation with doc ID
```

### DELIVERY NOTE WORKFLOW

```
Command: /delivery

Workflow Mirrors Purchase Receipt:
├── Customer selection (instead of supplier)
├── Item addition with quantities & rates
├── Warehouse assignment (source warehouse)
├── Submission to ERPNext
└── Status management (draft/submit/cancel)

Document States:
├── Draft .................. Editable, not shipped
├── Submitted .............. Inventory deducted
└── Cancelled .............. Shipment reversed
```

### CREDENTIAL MANAGEMENT

```
View Stored Credentials:
Command: /apic
Output: Displays masked API key (first 4 + last 4 chars visible)

Clear Credentials:
Command: /clear
Action: Removes stored credentials, requires re-authentication

Security Notes:
- Credentials stored encrypted in SQLite
- No plaintext credential storage
- Per-user isolation (user_id as key)
- Bot restart preserves credentials
```

---

## COMMAND REFERENCE

```
AUTHENTICATION & SETUP
/start ..................... Initialize bot, trigger credential input
/help ...................... Display command reference

DATA OPERATIONS
/items ..................... Open item search interface
/entry ..................... Stock entry menu (receipt/issue)
/purchase .................. Purchase receipt management
/delivery .................. Delivery note operations

ADMINISTRATIVE
/apic ...................... View stored API credentials (masked)
/clear ..................... Clear stored credentials
/cancel .................... Abort current workflow/conversation

Note: All commands require prior authentication via /start
      Commands can be triggered via inline keyboard buttons
```

---

## PROJECT STRUCTURE

```
stock-manager-bot/
├── docker-compose.yml ............... Orchestration configuration
├── Dockerfile ....................... Container image definition
├── Makefile ......................... Build automation
├── pyproject.toml ................... Python package metadata
├── README.md ........................ This documentation
├── .env.example ..................... Environment template
├── .gitignore ....................... Version control exclusions
│
└── stock_manager_bot/ ............... Application source
    ├── __init__.py .................. Package initialization
    ├── __main__.py .................. Entry point (python -m)
    ├── bot.py ....................... Core bot logic (3000+ lines)
    ├── config.py .................... Configuration loader
    ├── storage.py ................... SQLite database interface
    ├── purchase.py .................. Purchase receipt handlers
    └── delivery.py .................. Delivery note handlers

Data Persistence:
./data/ .............................. SQLite database location
└── stock_manager.sqlite3 ............ User credentials & state
```

---

## MAKEFILE AUTOMATION

```bash
make ................... Build and start (docker compose up --build)
make logs .............. Stream container logs (follow mode)
make down .............. Stop and remove containers
make restart ........... Equivalent to: make down && make

Advanced Usage:
make build ............. Build image without starting
make ps ................ List running containers
make exec .............. Open shell in container
```

---

## DIAGNOSTIC PROCEDURES

### ISSUE: Bot Not Responding

```
Diagnosis Sequence:
[1] Verify bot token
    $ echo $STOCK_BOT_TOKEN
    Expected: Numeric:Alphanumeric format
    
[2] Check bot process
    $ docker ps | grep stock_manager
    $ docker logs stock_manager_bot
    
[3] Test Telegram API connectivity
    $ curl https://api.telegram.org/bot$STOCK_BOT_TOKEN/getMe
    Expected: JSON with bot details
    
[4] Review application logs
    $ docker logs stock_manager_bot --tail 50

Resolution:
- Regenerate bot token if compromised (@BotFather /token)
- Verify firewall rules allow HTTPS to api.telegram.org
- Check Docker container health: docker inspect stock_manager_bot
```

### ISSUE: ERPNext API Connection Failures

```
Diagnosis Sequence:
[1] Verify ERPNext instance accessibility
    $ curl -I https://your-erp-domain.com
    Expected: HTTP 200 or 301/302
    
[2] Test API endpoint
    $ curl https://your-erp-domain.com/api/method/frappe.auth.get_logged_user \
      -H "Authorization: token API_KEY:API_SECRET"
    
[3] Check Docker DNS resolution
    $ docker exec stock_manager_bot nslookup your-erp-domain.com
    
[4] Review ERPNext API logs
    Navigate: ERPNext → Setup → Error Log

Resolution:
- Verify API access enabled in ERPNext System Settings
- Check user API key has not expired
- Confirm company name matches ERPNext Company DocType
- Review ERPNext User permissions for API user
```

### ISSUE: Database Locked Errors

```
Diagnosis:
SQLite database locked due to concurrent access or interrupted transaction

Resolution:
$ docker exec stock_manager_bot rm /app/data/stock_manager.sqlite3.lock
$ docker restart stock_manager_bot

Prevention:
- Avoid manual database file manipulation while bot running
- Use bot commands for all operations
- Regular database backups recommended
```

### DEPENDENCY ISSUES

```bash
# Rebuild container with fresh dependencies
make down
docker rmi stock-manager-bot:latest
make

# Local development dependency refresh
pip install --upgrade pip
pip install -e . --force-reinstall --no-cache-dir

# Verify installed versions
pip list | grep telegram
pip list | grep requests
```

---

## SECURITY CONSIDERATIONS

```
CREDENTIAL STORAGE
├── Encryption: SQLite database with application-level encryption
├── Isolation: Per-user credential partitioning (user_id indexed)
├── Transmission: HTTPS only (Telegram API enforced)
└── Lifetime: Persistent until user executes /clear

API KEY SECURITY
├── Validation: Server-side verification on each /start
├── Rate Limiting: Telegram Bot API rate limiter integrated
├── Rotation: Recommended 90-day rotation cycle
└── Scope: Limited to ERPNext API permissions

BEST PRACTICES
├── Never commit .env file to version control
├── Use environment-specific .env files (dev/prod)
├── Regularly update dependencies (pip install -U)
├── Monitor Docker logs for suspicious activity
├── Implement IP whitelisting on ERPNext firewall
└── Use separate ERPNext API users per bot instance
```

---

## DEVELOPMENT GUIDELINES

### CONTRIBUTION WORKFLOW

```
[1] Fork repository
[2] Clone to local development environment
[3] Create feature branch
    $ git checkout -b feature/description
    
[4] Implement changes
    - Follow PEP 8 style guidelines
    - Add type hints to all functions
    - Document complex logic with inline comments
    
[5] Test locally
    $ python -m stock_manager_bot
    
[6] Commit with descriptive messages
    Format: type(scope): description
    Example: feat(purchase): add multi-item support
    
[7] Push to fork
    $ git push origin feature/description
    
[8] Open pull request with detailed description
```

### CODE ARCHITECTURE

```
bot.py (Core)
├── StockManagerBot class ......... Main bot controller
├── Command handlers .............. /start, /entry, /purchase, etc.
├── Callback query router ......... Button press handling
├── Inline query processor ........ Search functionality
└── Error handler ................. Global exception catcher

purchase.py (Mixin)
├── PurchaseFlowMixin class ....... Purchase receipt workflows
├── Supplier selection ............ Inline query handler
├── Item addition loop ............ Multi-item support
└── Approval workflow ............. Submit/cancel operations

delivery.py (Mixin)
├── DeliveryFlowMixin class ....... Delivery note workflows
├── Customer selection ............ Inline query handler
├── Item addition loop ............ Multi-item support
└── Shipping workflow ............. Submit/cancel operations

storage.py
├── StockStorage class ............ SQLite interface
├── Credential CRUD operations .... Encrypted storage
├── Draft management .............. Temporary workflow state
└── Schema migrations ............. Database versioning

config.py
├── StockBotConfig dataclass ...... Configuration schema
├── Environment variable loader ... .env parsing
└── Validation logic .............. Required field checks
```

---

## ROADMAP & FUTURE ENHANCEMENTS

```
Planned Features:
├── Stock Reconciliation .......... Inventory count adjustments
├── Batch/Serial Number Support ... Advanced tracking
├── Multi-language Interface ...... I18n implementation
├── Webhook Mode .................. Alternative to long polling
├── Advanced Reporting ............ PDF generation via bot
└── Integration with ERPNext Notifications
```

---

## LICENSE

```
MIT License

Copyright (c) 2025 Abdulfattox Qurbonov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## SUPPORT & CONTRIBUTIONS

```
Issue Reporting:
Platform: GitHub Issues
URL: https://github.com/WIKKIwk/ERPNext_stock_manager_tg_integration/issues

Required Information:
├── ERPNext version
├── Python version
├── Docker version (if containerized)
├── Bot token validity status
├── Error messages (full traceback)
└── .env configuration (sanitized)

Response SLA:
├── Critical: 24 hours
├── High: 72 hours
└── Normal: Best effort

Pull Requests:
- Include tests for new features
- Update documentation accordingly
- Follow existing code style
- Provide clear commit messages
```

---

```
PROJECT: Stock Manager Bot for ERPNext
VERSION: 0.1.0
LAST_UPDATED: 2025-12-26
MAINTAINER: Abdulfattox Qurbonov
RUNTIME: Python 3.10+ | Telegram Bot API 21.2+
STATUS: PRODUCTION_READY
```

**END DOCUMENTATION**
