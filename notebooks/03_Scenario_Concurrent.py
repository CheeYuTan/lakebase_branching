# Databricks notebook source

# MAGIC %md
# MAGIC # ⚡ Scenario 3: Concurrent Changes — Production Drifted
# MAGIC
# MAGIC **Use case**: You're developing a feature on a branch, but meanwhile another team pushes
# MAGIC schema changes to production. When you try to promote your migration, you discover
# MAGIC production has drifted. How do you handle it?
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to detect that production has changed since your branch was created
# MAGIC - The **"re-branch and re-test"** pattern for handling drift
# MAGIC - How to write migrations that are resilient to concurrent changes
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ── another team adds email_verified ──── replay both migrations ── production (final)
# MAGIC        \                                                 ↑
# MAGIC         └── feature/order-priority                       │
# MAGIC              1. Add priority column                      │
# MAGIC              2. Discover drift!                          │
# MAGIC              3. Re-branch from updated production         │
# MAGIC              4. Re-test migration ───────────────────────┘
# MAGIC              5. 🗑️ cleanup
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
# MAGIC ## Step 1: Create Your Feature Branch
# MAGIC
# MAGIC You start developing the `order-priority` feature — adding a `priority` column to orders.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "feature-order-priority"
BRANCH_NAME_V2 = "feature-order-priority-v2"

# Clean up from previous runs
for bn in [BRANCH_NAME, BRANCH_NAME_V2]:
    try:
        w.postgres.delete_branch(name=f"projects/{project_name}/branches/{bn}").wait()
        print(f"🧹 Cleaned up existing branch '{bn}'")
    except Exception:
        pass

# Create your feature branch
print(f"\n🔄 Creating branch '{BRANCH_NAME}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)  # 48-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()
print(f"✅ Branch '{BRANCH_NAME}' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Develop Your Migration on the Branch
# MAGIC
# MAGIC You add a `priority` column to the `orders` table.

# COMMAND ----------

feature_conn, _, _ = connect_to_branch(BRANCH_NAME)

# Your migration: add priority to orders
YOUR_MIGRATION = f"""
ALTER TABLE {db_schema}.orders
ADD COLUMN IF NOT EXISTS priority VARCHAR(10) DEFAULT 'normal';

UPDATE {db_schema}.orders
SET priority = CASE
    WHEN total > 500 THEN 'high'
    WHEN total > 200 THEN 'medium'
    ELSE 'normal'
END;
"""

with feature_conn.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

print("✅ Your migration applied on feature branch!")

with feature_conn.cursor() as cur:
    cur.execute(f"""
        SELECT priority, COUNT(*) as cnt, ROUND(AVG(total), 2) as avg_total
        FROM {db_schema}.orders
        GROUP BY priority ORDER BY priority
    """)
    print(f"\n📊 Order priorities (on branch):")
    for row in cur.fetchall():
        print(f"   {row[0]:10s} {row[1]:4d} orders  (avg ${row[2]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Meanwhile... Another Team Changes Production
# MAGIC
# MAGIC While you were working on your branch, another team added an `email_verified` column
# MAGIC to the `customers` table on **production**. You don't know about this yet.

# COMMAND ----------

# Simulate another team's change on production
OTHER_TEAM_MIGRATION = f"""
ALTER TABLE {db_schema}.customers
ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE;

UPDATE {db_schema}.customers
SET email_verified = TRUE
WHERE id % 3 = 0;  -- roughly 1/3 verified
"""

with conn.cursor() as cur:
    cur.execute(OTHER_TEAM_MIGRATION)
    cur.execute(f"""
        SELECT email_verified, COUNT(*)
        FROM {db_schema}.customers
        GROUP BY email_verified
    """)
    print("📢 Another team pushed to production!")
    print(f"   Added 'email_verified' column to customers:")
    for row in cur.fetchall():
        status = "verified" if row[0] else "not verified"
        print(f"   • {status}: {row[1]} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Discover the Drift
# MAGIC
# MAGIC Before promoting your migration to production, you compare the schemas.
# MAGIC You discover that production has a column your branch doesn't!

# COMMAND ----------

# Get columns from production
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_columns = [row[0] for row in cur.fetchall()]

# Get columns from your branch
with feature_conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    branch_columns = [row[0] for row in cur.fetchall()]

# Compare
prod_only = set(prod_columns) - set(branch_columns)
branch_only = set(branch_columns) - set(prod_columns)

print("🔍 Schema comparison (customers table):")
print(f"   Production columns: {prod_columns}")
print(f"   Branch columns:     {branch_columns}")
print(f"")
if prod_only:
    print(f"   ⚠️  Columns on production but NOT on branch: {prod_only}")
if branch_only:
    print(f"   ⚠️  Columns on branch but NOT on production: {branch_only}")
print(f"\n🚨 Production has drifted! It has changes your branch doesn't know about.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Re-branch from Updated Main
# MAGIC
# MAGIC The safest approach: create a **new branch** from the current state of production (which includes
# MAGIC the other team's changes), and re-apply your migration on it.
# MAGIC
# MAGIC This is analogous to a `git rebase` — you replay your changes on top of the latest production.

# COMMAND ----------

# Create a new branch from the CURRENT production (which has email_verified)
print(f"🔄 Creating '{BRANCH_NAME_V2}' from current production (with other team's changes)...")

w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)
    )),
    branch_id=BRANCH_NAME_V2
).wait()

feature_conn_v2, _, _ = connect_to_branch(BRANCH_NAME_V2)
print(f"✅ Branch '{BRANCH_NAME_V2}' created from updated production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Re-apply Your Migration on the New Branch
# MAGIC
# MAGIC Since your migration is **idempotent** (`IF NOT EXISTS`), it's safe to replay.
# MAGIC The new branch already has `email_verified` from production, and now gets `priority` from you.

# COMMAND ----------

# Re-apply your migration
with feature_conn_v2.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

# Verify: the new branch should have BOTH changes
with feature_conn_v2.cursor() as cur:
    # Check customers columns (should have email_verified from production)
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    v2_customer_cols = [row[0] for row in cur.fetchall()]

    # Check orders columns (should have priority from your migration)
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    v2_order_cols = [row[0] for row in cur.fetchall()]

print(f"✅ Migration re-applied on '{BRANCH_NAME_V2}'!")
print(f"   customers columns: {v2_customer_cols}")
print(f"   → Has email_verified (from other team): {'email_verified' in v2_customer_cols}")
print(f"   orders columns: {v2_order_cols}")
print(f"   → Has priority (your change): {'priority' in v2_order_cols}")
print(f"\n🎉 Both changes coexist — no conflicts!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Promote to Production
# MAGIC
# MAGIC Now that we've validated our migration works alongside the other team's changes,
# MAGIC we can safely replay just our migration on production.
# MAGIC
# MAGIC > Production already has `email_verified`. We just need to add `priority`.

# COMMAND ----------

# Replay YOUR migration on production (it's idempotent, safe to run)
with conn.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

# Verify production has both changes
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    prod_order_cols = [row[0] for row in cur.fetchall()]

    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_customer_cols = [row[0] for row in cur.fetchall()]

print(f"✅ Migration promoted to production!")
print(f"   customers: {prod_customer_cols}")
print(f"   orders: {prod_order_cols}")
print(f"\n🎉 Production has both teams' changes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Cleanup
# MAGIC
# MAGIC > ⚠️ **This cell is skipped by default.** Remove `%skip` below to delete the branches now.

# COMMAND ----------

# MAGIC %skip

feature_conn.close()
feature_conn_v2.close()

for bn in [BRANCH_NAME, BRANCH_NAME_V2]:
    try:
        delete_branch_safe(bn)
    except Exception as e:
        print(f"   ('{bn}' already cleaned up)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Your branch** | Added `priority` column to orders |
# MAGIC | **Meanwhile** | Another team added `email_verified` to customers on production |
# MAGIC | **Detect drift** | Schema comparison revealed the gap |
# MAGIC | **Re-branch** | Created new branch from updated production |
# MAGIC | **Re-test** | Replayed your migration — validated it works alongside new changes |
# MAGIC | **Promote** | Replayed your migration on production (idempotent, safe) |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Always compare schemas** before promoting migrations
# MAGIC 2. **Write idempotent DDL** (`IF NOT EXISTS`, `IF EXISTS`) so migrations can be replayed
# MAGIC 3. **Re-branch from current production** when drift is detected (like `git rebase`)
# MAGIC 4. **Branches are cheap** — creating a v2 branch costs nothing (copy-on-write)
# MAGIC
# MAGIC > **Next**: Try **Scenario 4** (`04_Scenario_CICD_Ephemeral`) to see how to use
# MAGIC > short-lived branches for CI/CD pipelines with auto-expiration.
