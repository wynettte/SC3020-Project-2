# SC3020 Project 2 (Group 22)

## Requirements
- Python 3.10+
- PostgreSQL (with TPC-H dataset loaded)

---

## Installation

### Install required dependencies
```bash
pip install psycopg2-binary fastapi uvicorn PyQt6 python-dotenv
```

---

## Environment Setup

### Create a `.env` file in the project root:

```env
PGDATABASE=your_database_name
PGUSER=postgres
PGPASSWORD=your_password
PGHOST=localhost
PGPORT=5432
```

---

## Running the Application

### Start the GUI:
```bash
python project.py
```

---

## Sample Query

You can use the following query to test the application:

```sql
SELECT c.c_custkey, c.c_name, o.o_orderkey, o.o_totalprice
FROM customer c
JOIN orders o
  ON c.c_custkey = o.o_custkey
WHERE o.o_totalprice BETWEEN 1000 AND 5000;
```
