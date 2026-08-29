# Mini-RDBMS Engine 🗄️

**A Custom Relational Database Management System Built from First Principles in Python**

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Storage Engine: JSON/File-Backed](https://img.shields.io/badge/Storage-Persistent%20Disk%20I%2FO-brightgreen.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A lightweight relational database management engine implemented without external database libraries (such as SQLite or SQLAlchemy). Designed to demonstrate core database internals including query tokenization, structured file-backed storage, parameterized CRUD operations, and transaction integrity.

---

### ⚡ Core Capabilities

* **Lexical Parser & Query Execution:** Custom SQL tokenizer and parser handling `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE` commands with conditional `WHERE` clause evaluation.
* **Persistent File-Based Storage:** Structured disk persistence layer managing schema definitions, row serialization, and state across sessions.
* **Interactive Command-Line Interface (CLI):** Full REPL interface providing interactive querying, formatted tabular output, and operational execution status.
* **Integrity & Validation:** Enforces schema structure, input sanitization, and execution error-handling to prevent data corruption during disk operations.

---

### 🛠️ Project Architecture

```text
Pesapal-Mini-RDBMS/
├── src/                 # Core engine source modules
│   ├── parser.py        # SQL syntax tokenizer and validator
│   ├── storage.py       # Disk I/O, table serialization and persistence
│   └── execution.py     # Query execution planner and evaluation
├── data/                # Sample database tables and disk storage files
├── tests/               # Automated unit and integration test suite
├── main.py              # Interactive CLI REPL entry point
└── README.md            # Technical documentation

### 🚀 Quick Start & CLI Usage

#### 1. Clone & Run

```bash
git clone [https://github.com/KelvinNjiru/Pesapal-Mini-RDBMS.git](https://github.com/KelvinNjiru/Pesapal-Mini-RDBMS.git)
cd Pesapal-Mini-RDBMS
python3 main.py
```

#### 2. Example Query Execution

```sql
-- Create a new table
CREATE users id name role

-- Insert records
INSERT users 1 Kelvin "Network Engineer"
INSERT users 2 Alex "DevOps Specialist"

-- Query with conditions
SELECT users WHERE id=1

-- Update records
UPDATE users SET role="Lead Architect" WHERE id=1

-- Delete records
DELETE users WHERE id=2
```

---

### 🧪 Automated Testing

```bash
pytest tests/
```

---

### 👤 Author

**Kelvin Nyaga Njiru**  
*Network Automation Engineer | Systems Developer*  
* [LinkedIn](https://www.linkedin.com/in/kelvin-njiru)
* [Portfolio](https://kelvinjiru.netlify.app/)
