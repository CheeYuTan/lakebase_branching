# Databricks notebook source

# MAGIC %md
# MAGIC # 🚀 Notebook 00: Setup Lakebase Project & Seed Data
# MAGIC
# MAGIC This notebook creates a **Lakebase Autoscaling** project and seeds it with an
# MAGIC e-commerce database. All subsequent scenario notebooks (01–05) depend on this setup.
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Creates a new Lakebase project with autoscaling compute
# MAGIC 2. Waits for the project to become active
# MAGIC 3. Creates a database role with OAuth authentication (fully automated)
# MAGIC 4. Seeds 4 tables with realistic e-commerce data
# MAGIC 5. Verifies everything is ready
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - **Cluster**: Any Databricks cluster with Python 3.10+
# MAGIC - **Permissions**: `CAN MANAGE` on Lakebase
# MAGIC - **Region**: Workspace must be in a supported region:
# MAGIC   `us-east-1`, `us-east-2`, `eu-central-1`, `eu-west-1`, `eu-west-2`,
# MAGIC   `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`
# MAGIC
# MAGIC ## Architecture After Setup
# MAGIC ```
# MAGIC Lakebase Project: lakebase-branching-demo
# MAGIC └── main (default branch)
# MAGIC     ├── customers   (100 rows)
# MAGIC     ├── products    (50 rows)
# MAGIC     ├── orders      (200 rows)
# MAGIC     └── order_items (500 rows)
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) | [API Reference](https://docs.databricks.com/api/workspace/postgres)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration
# MAGIC
# MAGIC Set the widgets below to configure your project. The defaults work out of the box.
# MAGIC
# MAGIC | Widget | Description |
# MAGIC |---|---|
# MAGIC | `project_name` | Unique name for your Lakebase project. Keep this consistent across all notebooks. |
# MAGIC | `min_cu` | Minimum compute units (0.5 = smallest, cost-effective for demos) |
# MAGIC | `max_cu` | Maximum compute units (4.0 = enough for realistic workloads) |
# MAGIC | `suspend_timeout_seconds` | Auto-suspend idle compute after N seconds (60 = aggressive, saves cost) |

# COMMAND ----------

dbutils.widgets.text("project_name", "lakebase-branching-demo", "1. Project Name")
dbutils.widgets.text("min_cu", "0.5", "2. Min Compute Units")
dbutils.widgets.text("max_cu", "4.0", "3. Max Compute Units")
dbutils.widgets.text("suspend_timeout_seconds", "60", "4. Suspend Timeout (sec)")

# COMMAND ----------

# Read widget values
project_name = dbutils.widgets.get("project_name")
min_cu = float(dbutils.widgets.get("min_cu"))
max_cu = float(dbutils.widgets.get("max_cu"))
suspend_timeout_seconds = int(dbutils.widgets.get("suspend_timeout_seconds"))

print("📋 Configuration:")
print(f"   Project Name:      {project_name}")
print(f"   Min CU:            {min_cu}")
print(f"   Max CU:            {max_cu}")
print(f"   Suspend Timeout:   {suspend_timeout_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize the Databricks SDK
# MAGIC
# MAGIC The `WorkspaceClient` auto-authenticates when running inside a Databricks notebook —
# MAGIC no tokens or secrets needed.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print(f"✅ SDK initialized")
print(f"   Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Lakebase Project
# MAGIC
# MAGIC We'll create a new Lakebase project with autoscaling compute.
# MAGIC
# MAGIC **What happens under the hood:**
# MAGIC - A new PostgreSQL 17 instance is provisioned
# MAGIC - A default `main` branch is created automatically
# MAGIC - A compute endpoint is attached to the `main` branch
# MAGIC - Autoscaling is configured between `min_cu` and `max_cu`
# MAGIC - The compute auto-suspends after `suspend_timeout_seconds` of idle time
# MAGIC
# MAGIC > ⏱️ It may take a few moments for your compute to activate.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings, Duration
)

# Check if the project already exists
existing_projects = list(w.postgres.list_projects())
project_exists = any(
    p.name == f"projects/{project_name}" for p in existing_projects
)

if project_exists:
    print(f"ℹ️  Project '{project_name}' already exists — skipping creation.")
    print(f"   If you want a fresh start, run 99_Cleanup first.")
else:
    print(f"🔄 Creating project '{project_name}'...")
    print(f"   PostgreSQL version: 17")
    print(f"   Compute: {min_cu} – {max_cu} CU, auto-suspend after {suspend_timeout_seconds}s")
    
    result = w.postgres.create_project(
        project=Project(spec=ProjectSpec(
            display_name=project_name,
            pg_version=17,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
                suspend_timeout_duration=Duration(seconds=suspend_timeout_seconds)
            )
        )),
        project_id=project_name
    ).wait()
    
    print(f"\n✅ Project '{project_name}' created successfully!")

# Get project UID and display the Lakebase UI link
project_obj = next(
    p for p in w.postgres.list_projects()
    if p.name == f"projects/{project_name}"
)
project_uid = project_obj.uid
workspace_host = w.config.host.rstrip("/")
lakebase_url = f"{workspace_host}/lakebase/projects/{project_uid}"

print(f"\n🔗 Lakebase UI: {lakebase_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b: Verify Project & Get Main Branch
# MAGIC
# MAGIC Every Lakebase project comes with a default `main` branch. Let's confirm it exists
# MAGIC and get its compute endpoint (we'll need the host to connect via `psycopg2`).

# COMMAND ----------

import time

# List branches — the default 'main' branch should exist
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

print(f"📋 Branches in '{project_name}':")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    is_default = "⭐ default" if b.status and b.status.default else ""
    print(f"   • {branch_id} {is_default}")

# Get the main branch (the default one, or fallback to the first)
main_branch = next(
    (b for b in branches if b.status and b.status.default),
    branches[0]
)
main_branch_name = main_branch.name
print(f"\n✅ Main branch: {main_branch_name}")

# COMMAND ----------

# Get the compute endpoint for the main branch
endpoints = list(w.postgres.list_endpoints(parent=main_branch_name))

if not endpoints:
    print("⏳ Compute endpoint not ready yet. Waiting...")
    for i in range(30):
        time.sleep(10)
        endpoints = list(w.postgres.list_endpoints(parent=main_branch_name))
        if endpoints:
            break
        print(f"   Still waiting... ({(i+1)*10}s)")

if endpoints:
    main_endpoint = endpoints[0]
    main_endpoint_name = main_endpoint.name
    main_host = main_endpoint.status.hosts.host
    print(f"✅ Compute endpoint ready!")
    print(f"   Endpoint: {main_endpoint_name}")
    print(f"   Host: {main_host}")
    print(f"   Port: 5432")
    print(f"   Database: postgres")
else:
    raise Exception("Compute endpoint not available after 5 minutes. Check the Lakebase UI for project status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Database Role & Connect
# MAGIC
# MAGIC Lakebase supports **OAuth token-based authentication** — your Databricks identity is used
# MAGIC to generate short-lived database tokens. No passwords to manage!
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. We create a role linked to your Databricks identity via the SDK
# MAGIC 2. The SDK generates an OAuth token using `generate_database_credential`
# MAGIC 3. We connect via `psycopg2` using the token as the password
# MAGIC
# MAGIC > 💡 **Token lifetime**: Tokens auto-expire, so they're generated fresh each time.
# MAGIC > This is more secure than static passwords and fully automated.

# COMMAND ----------

from databricks.sdk.service.postgres import Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType

# Get current user identity
current_user = w.current_user.me()
username = current_user.user_name
print(f"👤 Current user: {username}")

# Create an OAuth-based role mapped to the current user
role_id = username.split("@")[0].replace(".", "_")  # e.g. "steven_tan"
print(f"🔄 Creating role '{role_id}' with OAuth authentication...")

try:
    role_result = w.postgres.create_role(
        parent=f"projects/{project_name}",
        role=Role(spec=RoleRoleSpec(
            auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
            identity_type=RoleIdentityType.USER,
            postgres_role=role_id,
        )),
        role_id=role_id
    ).wait()
    print(f"✅ Role '{role_id}' created successfully!")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"ℹ️  Role '{role_id}' already exists — reusing it.")
    else:
        print(f"❌ Error creating role: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to the database
# MAGIC
# MAGIC Now we generate an OAuth token and connect via `psycopg2`. This token acts as the
# MAGIC password — no manual credential management needed.

# COMMAND ----------

import psycopg2

# Generate a fresh OAuth token
cred = w.postgres.generate_database_credential(endpoint=main_endpoint_name)
db_token = cred.token
print(f"🔑 OAuth token generated (expires: {cred.expire_time})")

# Connect to the database
try:
    conn = psycopg2.connect(
        host=main_host,
        port=5432,
        dbname="postgres",
        user=role_id,
        password=db_token,
        sslmode="require"
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

    print(f"✅ Connected to Lakebase!")
    print(f"   PostgreSQL: {version[:60]}...")
    print(f"   Host: {main_host}")
    print(f"   User: {role_id}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print(f"\n   Troubleshooting:")
    print(f"   1. Is the endpoint active? Check the Lakebase UI.")
    print(f"   2. Does your user have permissions on this project?")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Seed the E-Commerce Schema
# MAGIC
# MAGIC We'll create 4 tables that model a simple e-commerce application:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────┐
# MAGIC │  customers   │     │   products   │
# MAGIC │──────────────│     │──────────────│
# MAGIC │ id (PK)      │     │ id (PK)      │
# MAGIC │ name         │     │ name         │
# MAGIC │ email        │     │ price        │
# MAGIC │ created_at   │     │ category     │
# MAGIC └──────┬───────┘     └──────┬───────┘
# MAGIC        │                     │
# MAGIC        │    ┌──────────────┐ │
# MAGIC        └───→│   orders     │←┘ (via order_items)
# MAGIC             │──────────────│
# MAGIC             │ id (PK)      │
# MAGIC             │ customer_id  │───→ customers.id
# MAGIC             │ total        │
# MAGIC             │ status       │
# MAGIC             │ created_at   │
# MAGIC             └──────┬───────┘
# MAGIC                    │
# MAGIC             ┌──────┴───────┐
# MAGIC             │ order_items  │
# MAGIC             │──────────────│
# MAGIC             │ id (PK)      │
# MAGIC             │ order_id     │───→ orders.id
# MAGIC             │ product_id   │───→ products.id
# MAGIC             │ quantity     │
# MAGIC             │ unit_price   │
# MAGIC             └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC > 💡 This schema is intentionally simple — the scenarios will evolve it
# MAGIC > (adding columns, backfilling data) to demonstrate branching workflows.

# COMMAND ----------

# --- Schema SQL (embedded for portability) ---

SEED_SCHEMA_SQL = """
-- Drop tables if they exist (idempotent)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Customers
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

-- Orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Order Items
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);
"""

with conn.cursor() as cur:
    cur.execute(SEED_SCHEMA_SQL)

print("✅ Schema created:")
print("   • customers")
print("   • products")
print("   • orders")
print("   • order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Sample Data
# MAGIC
# MAGIC We'll insert realistic e-commerce data:
# MAGIC - **100 customers** with unique names and emails
# MAGIC - **50 products** across 5 categories (Electronics, Clothing, Books, Home, Sports)
# MAGIC - **200 orders** with varying statuses (pending, confirmed, shipped, delivered)
# MAGIC - **~500 order items** linking orders to products
# MAGIC
# MAGIC > 💡 This data will be used across all scenarios. Scenario 2 will add a
# MAGIC > `loyalty_tier` column and backfill it based on order history.

# COMMAND ----------

import random

random.seed(42)  # Reproducible data

with conn.cursor() as cur:

    # --- Customers (100) ---
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", 
                   "Henry", "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia",
                   "Paul", "Quinn", "Ruby", "Sam", "Tara", "Uma", "Victor", "Wendy",
                   "Xander", "Yara", "Zach", "Amber", "Blake", "Cora", "Derek",
                   "Elena", "Felix", "Gina", "Hugo", "Isla", "Jake", "Kira", "Liam",
                   "Maya", "Nate", "Opal", "Pete", "Rosa", "Sean", "Tina", "Uri",
                   "Vera", "Wade", "Xena", "Yuri"]
    
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                  "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
                  "Jackson", "Martin"]
    
    customers = []
    for i in range(100):
        first = first_names[i % len(first_names)]
        last = last_names[i % len(last_names)]
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}.{i}@example.com"
        customers.append((name, email))
    
    cur.executemany(
        "INSERT INTO customers (name, email) VALUES (%s, %s)",
        customers
    )
    print(f"✅ Inserted {len(customers)} customers")

    # --- Products (50) ---
    categories = {
        "Electronics": ["Laptop", "Headphones", "Phone Case", "USB Cable", "Webcam",
                        "Keyboard", "Mouse", "Monitor", "Tablet", "Speaker"],
        "Clothing": ["T-Shirt", "Jeans", "Sneakers", "Jacket", "Hat",
                     "Scarf", "Socks", "Belt", "Hoodie", "Shorts"],
        "Books": ["Python Guide", "SQL Mastery", "Data Engineering", "ML Handbook", "Cloud Atlas",
                  "Clean Code", "System Design", "Algorithms", "DevOps Handbook", "AI Ethics"],
        "Home": ["Desk Lamp", "Coffee Mug", "Plant Pot", "Cushion", "Candle",
                 "Picture Frame", "Clock", "Vase", "Blanket", "Coaster"],
        "Sports": ["Yoga Mat", "Water Bottle", "Resistance Band", "Jump Rope", "Dumbbell",
                   "Tennis Ball", "Running Socks", "Gym Bag", "Towel", "Foam Roller"]
    }
    
    products = []
    for category, items in categories.items():
        for item in items:
            price = round(random.uniform(5.99, 299.99), 2)
            products.append((item, price, category))
    
    cur.executemany(
        "INSERT INTO products (name, price, category) VALUES (%s, %s, %s)",
        products
    )
    print(f"✅ Inserted {len(products)} products")

    # --- Orders (200) ---
    statuses = ["pending", "confirmed", "shipped", "delivered"]
    orders = []
    for i in range(200):
        customer_id = random.randint(1, 100)
        status = random.choice(statuses)
        total = 0  # Will update after order items
        orders.append((customer_id, total, status))
    
    cur.executemany(
        "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, %s)",
        orders
    )
    print(f"✅ Inserted {len(orders)} orders")

    # --- Order Items (~500) ---
    order_items = []
    for order_id in range(1, 201):
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.randint(1, 50)
            quantity = random.randint(1, 3)
            unit_price = products[product_id - 1][1]  # Price from products list
            order_items.append((order_id, product_id, quantity, unit_price))
    
    cur.executemany(
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
        order_items
    )
    print(f"✅ Inserted {len(order_items)} order items")

    # Update order totals based on actual items
    cur.execute("""
        UPDATE orders o SET total = sub.total
        FROM (
            SELECT order_id, SUM(quantity * unit_price) as total
            FROM order_items
            GROUP BY order_id
        ) sub
        WHERE o.id = sub.order_id
    """)
    print(f"✅ Updated order totals")

print(f"\n🎉 All sample data seeded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Setup
# MAGIC
# MAGIC Let's confirm everything is in place — tables exist, data is populated,
# MAGIC and the project is ready for the scenario notebooks.

# COMMAND ----------

print("=" * 60)
print(f"  PROJECT SUMMARY: {project_name}")
print("=" * 60)

with conn.cursor() as cur:
    # Table row counts
    tables = ["customers", "products", "orders", "order_items"]
    print("\n📊 Tables:")
    for table in tables:
        cur.execute(f"SELECT count(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"   • {table:20s} {count:>6} rows")

    # Sample data preview
    print("\n👤 Sample Customers (first 5):")
    cur.execute("SELECT id, name, email FROM customers ORDER BY id LIMIT 5")
    for row in cur.fetchall():
        print(f"   {row[0]:3d} | {row[1]:20s} | {row[2]}")

    # Order stats
    print("\n📦 Order Status Distribution:")
    cur.execute("""
        SELECT status, count(*) as cnt, ROUND(AVG(total), 2) as avg_total
        FROM orders GROUP BY status ORDER BY status
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:4d} orders  (avg ${row[2]})")

    # Top categories
    print("\n🏷️  Product Categories:")
    cur.execute("""
        SELECT category, count(*) as cnt, 
               ROUND(MIN(price), 2) as min_price,
               ROUND(MAX(price), 2) as max_price
        FROM products GROUP BY category ORDER BY category
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:15s} {row[1]:3d} products  (${row[2]} – ${row[3]})")

print("\n" + "=" * 60)
print(f"  ✅ Project '{project_name}' is READY!")
print("=" * 60)

# COMMAND ----------

# Close the connection
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 What's Next
# MAGIC
# MAGIC Your Lakebase project is set up and seeded with data. Here's what to run next:
# MAGIC
# MAGIC | Notebook | Scenario | What You'll Learn |
# MAGIC |---|---|---|
# MAGIC | **`01_Scenario_Data_Only`** | Prod data in dev (no schema changes) | Copy-on-write branching, data isolation |
# MAGIC | **`02_Scenario_Schema_To_Prod`** | Schema changes → production | Migration validation, promotion workflow |
# MAGIC | **`03_Scenario_Concurrent`** | Production drifted mid-development | Conflict detection, rebase pattern |
# MAGIC | **`04_Scenario_Point_In_Time`** | Point-in-time recovery | Instant rollback via time-travel branching |
# MAGIC | **`05_Scenario_CICD_Ephemeral`** | CI/CD ephemeral branches | Auto-expiration, TTL management |
# MAGIC
# MAGIC ### Important
# MAGIC - Keep **`project_name`** consistent across all notebooks
# MAGIC - Authentication uses **OAuth tokens** — no passwords to remember
# MAGIC - Scenarios 1 and 5 are standalone; Scenarios 3 and 4 require Scenario 2 first
# MAGIC
# MAGIC > 📖 **Full plan**: See `TECHNICAL_PLAN.md` in the repo for the complete architecture.
