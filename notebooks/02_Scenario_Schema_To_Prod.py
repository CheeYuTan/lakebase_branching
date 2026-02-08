# Databricks notebook source

# MAGIC %md
# MAGIC # 🔧 Scenario 2: Schema Changes — Dev to Production
# MAGIC
# MAGIC **Use case**: You need to add a new column (`loyalty_tier`) to the `customers` table,
# MAGIC backfill it with data, and promote the change to production — safely.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to develop schema changes (DDL) on a **feature branch**
# MAGIC - How to validate migrations before touching production
# MAGIC - How to promote validated schema changes to `production` by replaying DDL
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ─────────────────── replay migration ────── production (with loyalty_tier)
# MAGIC        \                           ↑
# MAGIC         └── feature/loyalty-tier   │
# MAGIC              1. ALTER TABLE        │
# MAGIC              2. Backfill data      │
# MAGIC              3. Validate ──────────┘
# MAGIC              4. 🗑️ delete branch
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Run Setup

# COMMAND ----------

# MAGIC %run ./00_Setup_Project

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create a Feature Branch
# MAGIC
# MAGIC We create a branch called `feature/loyalty-tier` from `production`.
# MAGIC This gives us a complete copy of the production database to develop against.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "feature-loyalty-tier"

# Clean up if exists from a previous run
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass

# Create the feature branch
print(f"🔄 Creating branch '{BRANCH_NAME}' from production...")

branch_result = w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=604800)  # 7-day TTL
    )),
    branch_id=BRANCH_NAME
).wait()

print(f"✅ Branch '{BRANCH_NAME}' created!")
print(f"   TTL: 7 days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to the Feature Branch

# COMMAND ----------

feature_conn, feature_host, feature_endpoint = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Develop the Schema Change on the Branch
# MAGIC
# MAGIC Our migration adds a `loyalty_tier` column to the `customers` table and backfills
# MAGIC it based on order history:
# MAGIC
# MAGIC | Tier | Criteria |
# MAGIC |---|---|
# MAGIC | `platinum` | 5+ orders |
# MAGIC | `gold` | 3-4 orders |
# MAGIC | `silver` | 1-2 orders |
# MAGIC | `bronze` | 0 orders |
# MAGIC
# MAGIC > 💡 We write the migration as **idempotent SQL** — it can be run multiple times safely.
# MAGIC > This is important because we'll replay it on `production` later.

# COMMAND ----------

# The migration script — idempotent and replayable
MIGRATION_SQL = f"""
-- Add loyalty_tier column (idempotent: IF NOT EXISTS)
ALTER TABLE {db_schema}.customers
ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'bronze';

-- Backfill loyalty tiers based on order history
UPDATE {db_schema}.customers c
SET loyalty_tier = CASE
    WHEN order_count >= 5 THEN 'platinum'
    WHEN order_count >= 3 THEN 'gold'
    WHEN order_count >= 1 THEN 'silver'
    ELSE 'bronze'
END
FROM (
    SELECT customer_id, COUNT(*) as order_count
    FROM {db_schema}.orders
    GROUP BY customer_id
) o
WHERE c.id = o.customer_id;
"""

with feature_conn.cursor() as cur:
    cur.execute(MIGRATION_SQL)

print("✅ Migration applied on feature branch!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate the Migration on the Branch
# MAGIC
# MAGIC Before promoting to production, let's verify the migration worked correctly.

# COMMAND ----------

with feature_conn.cursor() as cur:
    # Check the new column exists
    print("📋 Schema check — customers table columns:")
    cur.execute(f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        marker = " ← NEW" if row[0] == "loyalty_tier" else ""
        print(f"   {row[0]:20s} {row[1]:15s} {str(row[2] or ''):30s}{marker}")

    # Check loyalty tier distribution
    print(f"\n📊 Loyalty tier distribution:")
    cur.execute(f"""
        SELECT loyalty_tier, COUNT(*) as cnt
        FROM {db_schema}.customers
        GROUP BY loyalty_tier
        ORDER BY cnt DESC
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:4d} customers")

    # Verify data integrity — no NULLs
    cur.execute(f"SELECT COUNT(*) FROM {db_schema}.customers WHERE loyalty_tier IS NULL")
    null_count = cur.fetchone()[0]
    print(f"\n✅ Validation passed!")
    print(f"   • loyalty_tier column exists")
    print(f"   • All {100 - null_count}/100 customers have a tier assigned")
    print(f"   • No NULL values: {null_count == 0}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Compare Schemas in the UI
# MAGIC
# MAGIC Lakebase provides a built-in **Schema Diff** tool that lets you visually compare the schema
# MAGIC of a branch against its parent. This is a great way to review what changed before promoting.
# MAGIC
# MAGIC 👉 **Try it now:**
# MAGIC 1. Open the Lakebase UI (link printed below)
# MAGIC 2. Navigate to the `feature-loyalty-tier` branch
# MAGIC 3. Click the **Schema diff** button to see the differences vs production
# MAGIC
# MAGIC > 📖 **Docs**: [Compare branch schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas)
# MAGIC
# MAGIC Here's an example of what the Schema Diff looks like:
# MAGIC
# MAGIC ![Schema Comparison](/Workspace/Users/steven.tan@databricks.com/lakebase_branching/Compare_Schema.png)

# COMMAND ----------

# Print direct link to the branch in the Lakebase UI
branch_obj = w.postgres.get_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}")
branch_uid = branch_obj.uid
print(f"🔗 Open the branch in the Lakebase UI and click 'Schema diff':")
print(f"   {lakebase_url}/branches/{branch_uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Production is Untouched
# MAGIC
# MAGIC The schema change only exists on the branch. Production still has the original schema.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_columns = [row[0] for row in cur.fetchall()]

print(f"📋 Production branch columns: {prod_columns}")
print(f"   Has loyalty_tier? {'loyalty_tier' in prod_columns}")
print(f"")
print(f"✅ Production is untouched — schema change is isolated to the feature branch.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Promote Migration to Production (Replay on Production)
# MAGIC
# MAGIC Now we replay the **exact same DDL** on `production`:
# MAGIC
# MAGIC 1. We validated the migration on the branch ✅
# MAGIC 2. Now we replay the same idempotent DDL on `production`
# MAGIC 3. Since the SQL uses `IF NOT EXISTS`, it's safe to run multiple times

# COMMAND ----------

# Replay the exact same migration on production
with conn.cursor() as cur:
    cur.execute(MIGRATION_SQL)

print("✅ Migration replayed on production!")

# Verify on production
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT loyalty_tier, COUNT(*) as cnt
        FROM {db_schema}.customers
        GROUP BY loyalty_tier
        ORDER BY cnt DESC
    """)
    print(f"\n📊 Loyalty tiers on production:")
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:4d} customers")

print(f"\n🎉 Schema change successfully promoted to production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Cleanup — Delete the Feature Branch
# MAGIC
# MAGIC The feature branch has served its purpose. You can safely delete it, or let TTL handle it.
# MAGIC
# MAGIC > ⚠️ **This cell is skipped by default.** Remove `%skip` below to delete the branch now.

# COMMAND ----------

# MAGIC %skip

feature_conn.close()

delete_branch_safe(BRANCH_NAME)

# List remaining branches
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
print(f"\n📋 Remaining branches:")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    print(f"   • {branch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Branch** | Created `feature-loyalty-tier` from production (instant) |
# MAGIC | **Develop** | Added `loyalty_tier` column + backfill on branch |
# MAGIC | **Validate** | Verified schema, data integrity, tier distribution |
# MAGIC | **Isolate** | Confirmed production was untouched during development |
# MAGIC | **Promote** | Replayed the same idempotent DDL on production |
# MAGIC | **Cleanup** | Deleted the feature branch |
# MAGIC
# MAGIC ### The Migration Replay Pattern
# MAGIC ```
# MAGIC 1. Write idempotent DDL (ALTER TABLE ... IF NOT EXISTS, etc.)
# MAGIC 2. Test on branch → validate → fix if needed → re-test
# MAGIC 3. Once validated, replay the DDL on production
# MAGIC 4. Delete the branch
# MAGIC ```
# MAGIC
# MAGIC > **Next**: Try **Scenario 3** (`03_Scenario_Concurrent`) to see what happens when
# MAGIC > production changes while you're developing on a branch.
