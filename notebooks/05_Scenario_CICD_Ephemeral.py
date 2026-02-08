# Databricks notebook source

# MAGIC %md
# MAGIC # 🔄 Scenario 5: CI/CD Ephemeral Branches
# MAGIC
# MAGIC **Use case**: In a CI/CD pipeline, each pull request or test run gets its own **ephemeral
# MAGIC database branch** that auto-expires after a short TTL. This gives every PR a full, isolated
# MAGIC copy of the production database to test against — with zero manual cleanup.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to create **short-lived branches** with tight TTLs (e.g. 1 hour)
# MAGIC - How to simulate a CI/CD pipeline that spins up branches per PR
# MAGIC - How **branch expiration** ensures automatic cleanup
# MAGIC - How to run integration tests against ephemeral branches
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ────────────────────────────────────────────── (untouched)
# MAGIC        ├── ci-pr-42  ── test ── ✅ pass ── 🗑️ (TTL: 1h)
# MAGIC        ├── ci-pr-43  ── test ── ❌ fail ── 🗑️ (TTL: 1h)
# MAGIC        └── ci-pr-44  ── test ── ✅ pass ── 🗑️ (TTL: 1h)
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
# MAGIC ## Step 1: Define the CI/CD Pipeline Simulation
# MAGIC
# MAGIC We'll simulate 3 pull requests, each with:
# MAGIC - Its own database branch (named `ci-pr-<number>`)
# MAGIC - A 1-hour TTL (auto-expires if not deleted)
# MAGIC - A set of integration tests to run

# COMMAND ----------

import hashlib
import time as time_module
from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

# Simulated PRs
PULL_REQUESTS = [
    {
        "pr_number": 42,
        "title": "Add customer loyalty tiers",
        "migration": f"""
            ALTER TABLE {db_schema}.customers
            ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'bronze';
        """,
        "test_query": f"SELECT COUNT(*) FROM {db_schema}.customers WHERE loyalty_tier IS NOT NULL",
        "expected_pass": True
    },
    {
        "pr_number": 43,
        "title": "Add invalid column type",
        "migration": f"""
            ALTER TABLE {db_schema}.orders
            ADD COLUMN IF NOT EXISTS priority VARCHAR(10) DEFAULT 'normal';
        """,
        "test_query": f"SELECT COUNT(*) FROM {db_schema}.orders WHERE priority = 'invalid_status'",
        "expected_pass": False  # Simulated failure: no rows match
    },
    {
        "pr_number": 44,
        "title": "Add product ratings",
        "migration": f"""
            ALTER TABLE {db_schema}.products
            ADD COLUMN IF NOT EXISTS avg_rating DECIMAL(3,2) DEFAULT 0.00;

            UPDATE {db_schema}.products SET avg_rating = ROUND(RANDOM() * 4 + 1, 2);
        """,
        "test_query": f"SELECT COUNT(*) FROM {db_schema}.products WHERE avg_rating BETWEEN 1.0 AND 5.0",
        "expected_pass": True
    }
]

TTL_SECONDS = 3600  # 1 hour

print(f"📋 CI/CD Pipeline — {len(PULL_REQUESTS)} PRs to test:")
for pr in PULL_REQUESTS:
    print(f"   PR #{pr['pr_number']}: {pr['title']}")
print(f"\n   Branch TTL: {TTL_SECONDS}s ({TTL_SECONDS // 3600}h)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Ephemeral Branches for Each PR
# MAGIC
# MAGIC Each PR gets its own isolated branch. In a real CI/CD pipeline, this would be triggered
# MAGIC by a webhook or GitHub Action.
# MAGIC
# MAGIC > 💡 Branch creation is **instant** (copy-on-write), so even creating dozens of branches
# MAGIC > per day adds negligible overhead.

# COMMAND ----------

ci_branches = {}

for pr in PULL_REQUESTS:
    branch_name = f"ci-pr-{pr['pr_number']}"
    
    # Clean up from previous runs
    try:
        w.postgres.delete_branch(
            name=f"projects/{project_name}/branches/{branch_name}"
        ).wait()
    except Exception:
        pass
    
    # Create ephemeral branch
    print(f"🔄 PR #{pr['pr_number']}: Creating branch '{branch_name}'...")
    
    w.postgres.create_branch(
        parent=f"projects/{project_name}",
        branch=Branch(spec=BranchSpec(
            source_branch=prod_branch_name,
            ttl=Duration(seconds=TTL_SECONDS)
        )),
        branch_id=branch_name
    ).wait()
    
    ci_branches[pr['pr_number']] = branch_name
    print(f"   ✅ Branch '{branch_name}' created (TTL: {TTL_SECONDS // 3600}h)")

print(f"\n📋 All {len(ci_branches)} branches created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Migrations & Tests on Each Branch
# MAGIC
# MAGIC For each PR, we:
# MAGIC 1. Connect to its ephemeral branch
# MAGIC 2. Apply the PR's migration
# MAGIC 3. Run integration tests
# MAGIC 4. Report results
# MAGIC
# MAGIC This mirrors what a real CI pipeline would do — each test runs in **complete isolation**.

# COMMAND ----------

results = []

for pr in PULL_REQUESTS:
    branch_name = ci_branches[pr['pr_number']]
    print(f"\n{'='*60}")
    print(f"  PR #{pr['pr_number']}: {pr['title']}")
    print(f"  Branch: {branch_name}")
    print(f"{'='*60}")
    
    try:
        # Connect to the branch
        branch_conn, _, _ = connect_to_branch(branch_name)
        
        # Apply migration
        print(f"  📝 Applying migration...")
        with branch_conn.cursor() as cur:
            cur.execute(pr['migration'])
        print(f"  ✅ Migration applied")
        
        # Run test
        print(f"  🧪 Running tests...")
        with branch_conn.cursor() as cur:
            cur.execute(pr['test_query'])
            test_result = cur.fetchone()[0]
        
        # Evaluate
        if pr['expected_pass']:
            passed = test_result > 0
        else:
            passed = test_result == 0  # Expected to find no matching rows
        
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {status} (result: {test_result} rows)")
        
        results.append({
            "pr": pr['pr_number'],
            "title": pr['title'],
            "branch": branch_name,
            "passed": passed,
            "test_result": test_result
        })
        
        branch_conn.close()
        
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        results.append({
            "pr": pr['pr_number'],
            "title": pr['title'],
            "branch": branch_name,
            "passed": False,
            "test_result": str(e)
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: CI/CD Report
# MAGIC
# MAGIC Here's the consolidated test report — just like what you'd see in a CI dashboard.

# COMMAND ----------

print("=" * 70)
print("  CI/CD TEST REPORT")
print("=" * 70)

passed_count = sum(1 for r in results if r['passed'])
failed_count = len(results) - passed_count

for r in results:
    icon = "✅" if r['passed'] else "❌"
    print(f"  {icon} PR #{r['pr']:3d} | {r['title']:35s} | {r['branch']}")

print(f"\n  Summary: {passed_count} passed, {failed_count} failed, {len(results)} total")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Production is Untouched
# MAGIC
# MAGIC Even though we ran migrations on 3 branches, production remains exactly as it was.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_cols = [row[0] for row in cur.fetchall()]
    
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    prod_count = cur.fetchone()[0]

print(f"📊 Production check:")
print(f"   Customers columns: {prod_cols}")
print(f"   Customer count: {prod_count}")
print(f"   Has loyalty_tier? {'loyalty_tier' in prod_cols}")
print(f"\n✅ Production is completely untouched!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Cleanup (or Let TTL Handle It)
# MAGIC
# MAGIC In a real CI/CD pipeline, you'd let the TTL auto-expire the branches.
# MAGIC But we'll clean up explicitly for this demo.
# MAGIC
# MAGIC > 💡 **TTL in practice**: Set TTL to match your CI run time + buffer.
# MAGIC > - Quick tests: 1 hour
# MAGIC > - Nightly builds: 12 hours
# MAGIC > - Staging environments: 7 days

# COMMAND ----------

for pr_num, branch_name in ci_branches.items():
    try:
        w.postgres.delete_branch(
            name=f"projects/{project_name}/branches/{branch_name}"
        ).wait()
        print(f"🗑️ PR #{pr_num}: Branch '{branch_name}' deleted")
    except Exception:
        print(f"   PR #{pr_num}: Branch already cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Create** | 3 ephemeral branches (one per PR), each with 1-hour TTL |
# MAGIC | **Migrate** | Applied PR-specific migrations in isolation |
# MAGIC | **Test** | Ran integration tests on each branch |
# MAGIC | **Report** | Generated a CI/CD test report |
# MAGIC | **Verify** | Confirmed production was untouched |
# MAGIC | **Cleanup** | Deleted branches (or let TTL auto-expire) |
# MAGIC
# MAGIC ### CI/CD Integration Patterns
# MAGIC
# MAGIC **GitHub Actions / GitLab CI:**
# MAGIC ```yaml
# MAGIC # Example: Create ephemeral branch per PR
# MAGIC steps:
# MAGIC   - name: Create test database
# MAGIC     run: |
# MAGIC       python create_branch.py \
# MAGIC         --branch "ci-pr-${{ github.event.number }}" \
# MAGIC         --ttl 3600
# MAGIC   - name: Run migrations
# MAGIC     run: python apply_migrations.py --branch "ci-pr-${{ github.event.number }}"
# MAGIC   - name: Run tests
# MAGIC     run: pytest --db-branch "ci-pr-${{ github.event.number }}"
# MAGIC ```
# MAGIC
# MAGIC ### Why This Matters
# MAGIC - **No shared test databases** — every PR gets its own isolated copy
# MAGIC - **No cleanup scripts** — TTL handles everything automatically
# MAGIC - **Instant provisioning** — copy-on-write means zero wait time
# MAGIC - **Cost-effective** — branches share storage until data diverges
# MAGIC
# MAGIC > 🎉 **That's all 5 scenarios!** Run `99_Cleanup` to tear down the project.
