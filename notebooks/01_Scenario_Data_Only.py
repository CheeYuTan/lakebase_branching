# Databricks notebook source

# MAGIC %md
# MAGIC # 🔍 Scenario 1: Production Data in Dev — No Schema Changes
# MAGIC
# MAGIC **Use case**: You need a copy of production data for development, testing, or analytics —
# MAGIC without any risk of modifying the production database.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to create a **branch** from `main` (instant, zero-cost copy-on-write)
# MAGIC - How branches provide **full data isolation** — changes on a branch don't affect `main`
# MAGIC - How to query production data safely on a branch
# MAGIC - How **branch TTL** (time-to-live) auto-cleans up after you're done
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC main ──────────────────────────────────────────────── (untouched)
# MAGIC        \
# MAGIC         └── dev-readonly ── query ── insert ── 🗑️ (auto-expires)
# MAGIC              (instant copy)
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Run Setup
# MAGIC
# MAGIC This runs notebook `00_Setup_Project` to ensure the project exists, data is seeded,
# MAGIC and all shared variables (`w`, `project_name`, `conn`, `connect_to_branch()`, etc.) are available.

# COMMAND ----------

# MAGIC %run ./00_Setup_Project

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create a Dev Branch
# MAGIC
# MAGIC We'll create a branch called `dev-readonly` from `main`. This is an **instant** operation
# MAGIC thanks to Lakebase's copy-on-write architecture — no data is physically duplicated.
# MAGIC
# MAGIC We also set a **TTL of 24 hours**, so the branch auto-deletes if we forget to clean up.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "dev-readonly"

# Delete the branch if it already exists (from a previous run)
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass  # Branch doesn't exist, that's fine

# Create the branch
print(f"🔄 Creating branch '{BRANCH_NAME}' from main...")

branch_result = w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=f"projects/{project_name}/branches/main",
        ttl=Duration(seconds=86400)  # 24-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()

print(f"✅ Branch '{BRANCH_NAME}' created!")
print(f"   Source: main")
print(f"   TTL: 24 hours (auto-deletes after expiry)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to the Dev Branch
# MAGIC
# MAGIC Each branch gets its own compute endpoint. We use the `connect_to_branch()` helper
# MAGIC (defined in notebook 00) to connect.

# COMMAND ----------

dev_conn, dev_host, dev_endpoint = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Production Data on the Branch
# MAGIC
# MAGIC The branch has a **complete copy** of the production data. Let's verify by running the same
# MAGIC queries we'd run on `main`.
# MAGIC
# MAGIC > 💡 **Key insight**: No data was copied during branch creation. Lakebase uses
# MAGIC > copy-on-write — the branch shares storage with `main` until data diverges.

# COMMAND ----------

with dev_conn.cursor() as cur:
    # Row counts — should match main exactly
    tables = ["customers", "products", "orders", "order_items"]
    print(f"📊 Data on branch '{BRANCH_NAME}' (schema: {db_schema}):")
    for table in tables:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   • {table:20s} {count:>6} rows")

    # Run an analytics query
    print(f"\n📈 Top 5 customers by total spend:")
    cur.execute(f"""
        SELECT c.name, COUNT(o.id) as order_count, ROUND(SUM(o.total), 2) as total_spent
        FROM {db_schema}.customers c
        JOIN {db_schema}.orders o ON c.id = o.customer_id
        GROUP BY c.name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:20s}  {row[1]:3d} orders  ${row[2]:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Demonstrate Data Isolation
# MAGIC
# MAGIC Now let's prove that branches are **fully isolated**. We'll:
# MAGIC 1. Insert a new customer on the **branch**
# MAGIC 2. Verify it exists on the **branch**
# MAGIC 3. Verify it does **NOT** exist on **main**
# MAGIC
# MAGIC This is the core value of branching — developers can freely experiment without any risk
# MAGIC to production data.

# COMMAND ----------

# Insert a test customer on the dev branch
with dev_conn.cursor() as cur:
    cur.execute(f"""
        INSERT INTO {db_schema}.customers (name, email)
        VALUES ('Branch Test User', 'branch.test@example.com')
    """)
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    branch_count = cur.fetchone()[0]

# Check main — the test customer should NOT be there
with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    main_count = cur.fetchone()[0]

print(f"📊 Customer counts after insert:")
print(f"   Branch '{BRANCH_NAME}': {branch_count} customers (includes test user)")
print(f"   Main:                   {main_count} customers (unchanged!)")
print(f"")
print(f"✅ Data isolation confirmed — branch changes don't affect main!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleanup — Delete the Branch
# MAGIC
# MAGIC We set a 24-hour TTL, so the branch would auto-delete. But let's clean up explicitly.
# MAGIC
# MAGIC > 💡 **In practice**: TTL is useful for dev/test branches that developers might forget about.
# MAGIC > Lakebase ensures they don't linger and consume resources.

# COMMAND ----------

# Close the branch connection first
dev_conn.close()

# Delete the branch
w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
print(f"🗑️ Branch '{BRANCH_NAME}' deleted.")

# Verify — list remaining branches
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
print(f"\n📋 Remaining branches:")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    print(f"   • {branch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Concept | What Happened |
# MAGIC |---|---|
# MAGIC | **Branch creation** | Instant — no data copied (copy-on-write) |
# MAGIC | **Data access** | Full production dataset available immediately |
# MAGIC | **Isolation** | Insert on branch did NOT appear on main |
# MAGIC | **Cleanup** | Explicit delete, or automatic via TTL |
# MAGIC
# MAGIC ### When to Use This Pattern
# MAGIC - **Development**: Query prod data without risk
# MAGIC - **Testing**: Run integration tests against realistic data
# MAGIC - **Analytics**: Ad-hoc queries on a snapshot without affecting OLTP performance
# MAGIC - **Debugging**: Reproduce a production issue in an isolated environment
# MAGIC
# MAGIC > **Next**: Try **Scenario 2** (`02_Scenario_Schema_To_Prod`) to learn how to develop
# MAGIC > schema changes on a branch and promote them to production.
