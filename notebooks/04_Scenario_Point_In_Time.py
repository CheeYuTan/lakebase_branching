# Databricks notebook source

# MAGIC %md
# MAGIC # ⏪ Scenario 4: Point-in-Time Recovery
# MAGIC
# MAGIC **Use case**: Someone accidentally deletes or corrupts production data. Instead of restoring
# MAGIC from a backup (slow, disruptive), you create a branch from a **past point in time** to
# MAGIC instantly recover the data.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How **point-in-time branching** works (branch from a past timestamp)
# MAGIC - How to recover data without any downtime on production
# MAGIC - How to selectively restore specific rows instead of a full rollback
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ── data ok ──── 💥 DELETE ──── restore rows from branch ──── production (recovered)
# MAGIC                 │                                   ↑
# MAGIC                 │     ┌─────────────────────────────┘
# MAGIC                 └─── recovery-branch (from T-before-delete)
# MAGIC                       └── data still intact here!
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
# MAGIC ## Step 1: Record the Current State (Before Disaster)
# MAGIC
# MAGIC First, let's capture a timestamp **before** the accidental deletion occurs.
# MAGIC We'll use this timestamp to create a recovery branch later.

# COMMAND ----------

from datetime import datetime, timezone, timedelta

# Record the current time — this is our "safe point"
# We use UTC to match Lakebase's timestamp format
safe_timestamp = datetime.now(timezone.utc)
print(f"⏱️  Safe timestamp recorded: {safe_timestamp.isoformat()}")

# Count current data
with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    before_count = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(DISTINCT customer_id) as active_customers,
               COUNT(*) as total_orders,
               ROUND(SUM(total), 2) as total_revenue
        FROM {db_schema}.orders
    """)
    stats = cur.fetchone()

print(f"\n📊 Current production state:")
print(f"   Customers:        {before_count}")
print(f"   Active customers: {stats[0]}")
print(f"   Total orders:     {stats[1]}")
print(f"   Total revenue:    ${stats[2]}")
print(f"\n💾 Everything looks good. This is our 'known good' state.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 💥 Simulate an Accidental Deletion
# MAGIC
# MAGIC Someone runs a bad query that deletes a large chunk of customer data.
# MAGIC In real life, this could be a missing `WHERE` clause, a bad script, or a bug.
# MAGIC
# MAGIC > ⚠️ We wait a few seconds after the safe timestamp to ensure the point-in-time
# MAGIC > branch captures the state **before** the deletion.

# COMMAND ----------

import time

# Small delay to ensure timestamp separation
print("⏳ Waiting 5 seconds to ensure timestamp separation...")
time.sleep(5)

# THE DISASTER: accidentally delete customers with id > 50
with conn.cursor() as cur:
    cur.execute(f"DELETE FROM {db_schema}.order_items WHERE order_id IN (SELECT id FROM {db_schema}.orders WHERE customer_id > 50)")
    cur.execute(f"DELETE FROM {db_schema}.orders WHERE customer_id > 50")
    cur.execute(f"DELETE FROM {db_schema}.customers WHERE id > 50")
    
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    after_count = cur.fetchone()[0]

print(f"💥 DISASTER! Accidental deletion occurred!")
print(f"   Customers before: {before_count}")
print(f"   Customers after:  {after_count}")
print(f"   Rows lost:        {before_count - after_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Recovery Branch (Point-in-Time)
# MAGIC
# MAGIC This is where Lakebase shines. Instead of restoring from a backup (which could take hours
# MAGIC and cause downtime), we create a branch from the **exact moment before the deletion**.
# MAGIC
# MAGIC The `source_branch_time` parameter tells Lakebase to branch from a past timestamp.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration
from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp

BRANCH_NAME = "recovery-branch"

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass

# Convert the safe timestamp to protobuf Timestamp
pb_ts = PbTimestamp()
pb_ts.FromDatetime(safe_timestamp)

print(f"🔄 Creating recovery branch from timestamp: {safe_timestamp.isoformat()}")

branch_result = w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        source_branch_time=pb_ts,
        ttl=Duration(seconds=86400)  # 24-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()

print(f"✅ Recovery branch created! (instant — no backup restore needed)")
print(f"   Branch: {BRANCH_NAME}")
print(f"   Point-in-time: {safe_timestamp.isoformat()}")
print(f"   TTL: 24 hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Data on the Recovery Branch
# MAGIC
# MAGIC The recovery branch should have the **full dataset** as it existed at our safe timestamp —
# MAGIC before the accidental deletion.

# COMMAND ----------

recovery_conn, _, _ = connect_to_branch(BRANCH_NAME)

with recovery_conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    recovery_count = cur.fetchone()[0]

with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    prod_count = cur.fetchone()[0]

print(f"📊 Comparison:")
print(f"   Production (after disaster): {prod_count} customers")
print(f"   Recovery branch:          {recovery_count} customers")
print(f"   Recoverable rows:            {recovery_count - prod_count}")
print(f"\n✅ The recovery branch has the complete dataset from before the deletion!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Restore Data to Production
# MAGIC
# MAGIC Now we selectively restore the deleted rows from the recovery branch back to production.
# MAGIC
# MAGIC > 💡 **Selective restore**: We only copy back the missing rows, not a full table overwrite.
# MAGIC > Production stays online the entire time — zero downtime.
# MAGIC
# MAGIC Since we can't do cross-database queries between branches directly, we extract the missing
# MAGIC data from the recovery branch and insert it into production.

# COMMAND ----------

# Extract the deleted customers from the recovery branch
with recovery_conn.cursor() as cur:
    cur.execute(f"SELECT id, name, email FROM {db_schema}.customers WHERE id > 50 ORDER BY id")
    deleted_customers = cur.fetchall()
    print(f"📋 Found {len(deleted_customers)} customers to restore")

# Extract their orders
with recovery_conn.cursor() as cur:
    cur.execute(f"""
        SELECT id, customer_id, total, status
        FROM {db_schema}.orders WHERE customer_id > 50 ORDER BY id
    """)
    deleted_orders = cur.fetchall()
    print(f"📋 Found {len(deleted_orders)} orders to restore")

# Extract their order items
with recovery_conn.cursor() as cur:
    cur.execute(f"""
        SELECT oi.id, oi.order_id, oi.product_id, oi.quantity, oi.unit_price
        FROM {db_schema}.order_items oi
        JOIN {db_schema}.orders o ON oi.order_id = o.id
        WHERE o.customer_id > 50
        ORDER BY oi.id
    """)
    deleted_items = cur.fetchall()
    print(f"📋 Found {len(deleted_items)} order items to restore")

# COMMAND ----------

# Restore to production — insert the missing rows back
with conn.cursor() as cur:
    # Restore customers (use overriding to preserve original IDs)
    for c in deleted_customers:
        cur.execute(
            f"INSERT INTO {db_schema}.customers (id, name, email) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            c
        )
    print(f"✅ Restored {len(deleted_customers)} customers")

    # Restore orders
    for o in deleted_orders:
        cur.execute(
            f"INSERT INTO {db_schema}.orders (id, customer_id, total, status) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            o
        )
    print(f"✅ Restored {len(deleted_orders)} orders")

    # Restore order items
    for oi in deleted_items:
        cur.execute(
            f"INSERT INTO {db_schema}.order_items (id, order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            oi
        )
    print(f"✅ Restored {len(deleted_items)} order items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Recovery

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    final_count = cur.fetchone()[0]

print(f"📊 Recovery complete!")
print(f"   Before disaster:  {before_count} customers")
print(f"   After disaster:   {prod_count} customers")
print(f"   After recovery:   {final_count} customers")
print(f"")
if final_count == before_count:
    print(f"✅ Full recovery! All {before_count} customers restored.")
else:
    print(f"⚠️  Partial recovery: {final_count}/{before_count} customers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Cleanup

# COMMAND ----------

recovery_conn.close()

delete_branch_safe(BRANCH_NAME)
print(f"   Recovery complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Record** | Captured a "safe" timestamp before the disaster |
# MAGIC | **Disaster** | Accidentally deleted 50 customers + related orders |
# MAGIC | **Branch** | Created a point-in-time branch from before the deletion (instant!) |
# MAGIC | **Verify** | Confirmed the recovery branch had all the data |
# MAGIC | **Restore** | Selectively copied missing rows back to production |
# MAGIC | **Confirm** | Verified full recovery — zero downtime |
# MAGIC
# MAGIC ### Point-in-Time Branching vs Traditional Backup
# MAGIC | | Traditional Backup | Lakebase Point-in-Time |
# MAGIC |---|---|---|
# MAGIC | **Recovery time** | Minutes to hours | Seconds (instant branch) |
# MAGIC | **Downtime** | Often required | Zero |
# MAGIC | **Granularity** | Full restore only | Selective row recovery |
# MAGIC | **Cost** | Full data copy | Copy-on-write (minimal) |
# MAGIC
# MAGIC > **Next**: Try **Scenario 5** (`05_Scenario_CICD_Ephemeral`) to see how to use
# MAGIC > short-lived branches for CI/CD pipelines with auto-expiration.
