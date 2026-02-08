# Database Branching for Postgres: A Deep Dive into Databricks Lakebase

*How to create instant, zero-cost copies of your production database for development, testing, and CI/CD — with auto-expiration built in.*

---

If you've ever needed a copy of your production database to test a migration, debug an issue, or run integration tests — you know the pain. Traditional approaches involve `pg_dump`, hours of waiting, and gigabytes of duplicated storage. What if creating a full copy of your production database was **instant**, **free**, and **self-cleaning**?

That's exactly what **database branching** in Databricks Lakebase delivers.

In this post, I'll walk through four real-world branching scenarios using a simple e-commerce database. Every example comes with runnable Databricks notebooks that you can clone and reproduce in your own environment. Let's dive in.

---

## What Is Databricks Lakebase?

[Databricks Lakebase](https://docs.databricks.com/aws/en/oltp/index.html) is a fully managed PostgreSQL-compatible database service with autoscaling, scale-to-zero, and — the star of this post — **database branching**. Think of it as serverless Postgres with Git-like superpowers.

Key capabilities:
- **Autoscaling compute** (0.5 – 4.0 CU) that scales to zero when idle
- **Database branching** — instant, copy-on-write clones of your database
- **Branch TTL** — automatic expiration and cleanup of branches
- **OAuth authentication** — no passwords, just your Databricks identity
- **Full PostgreSQL 17** compatibility

## What Is Database Branching?

If you're familiar with Git, you already understand the mental model. A **database branch** is an instant, isolated copy of your entire database — schema, data, and all. Under the hood, Lakebase uses **copy-on-write** storage, which means:

- **Branch creation is instant** — no data is physically copied
- **Branches share storage** with the parent until data diverges
- **Changes are isolated** — writes on a branch never affect the parent
- **Branches are disposable** — set a TTL and they auto-delete

This opens up workflows that were previously impractical with traditional databases. Let me show you four of them.

---

## The Demo Setup

All scenarios use a simple e-commerce schema with four tables:

```
Lakebase Project: lakebase-branching-<your-username>
└── production (default branch)
    └── ecommerce (schema)
        ├── customers   (100 rows)
        ├── products    (50 rows)
        ├── orders      (200 rows)
        └── order_items (500 rows)
```

The setup notebook (`00_Setup_Project`) creates the project, seeds the data, and defines helper functions that all scenario notebooks use via `%run`. Every notebook is self-contained — just run it and everything bootstraps automatically.

> 🔗 **Full source code**: [GitHub repository](https://github.com/CheeYuTan/lakebase_branching) — clone it, connect to your Databricks workspace, and run.

---

## Scenario 1: Production Data in Dev — No Schema Changes

**The problem**: You need a copy of production data for development or analytics, but you can't risk modifying the real thing.

**The solution**: Create a branch. It takes less than a second.

```
production ─────────────────────────────────── (untouched)
       \
        └── dev-readonly ── query ── insert ── 🗑️ (auto-expires)
             (instant copy)
```

### How it works

First, we create a branch from production with a 24-hour TTL:

```python
from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

branch_result = w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=86400)  # 24-hour TTL
    )),
    branch_id="dev-readonly"
).wait()
```

That's it. The branch has a **complete copy** of all production data — 100 customers, 50 products, 200 orders — available immediately. No dump, no restore, no waiting.

### Proving isolation

The real power shows when we insert data on the branch:

```python
# Insert on the branch
with dev_conn.cursor() as cur:
    cur.execute("""
        INSERT INTO ecommerce.customers (name, email)
        VALUES ('Branch Test User', 'branch.test@example.com')
    """)
    cur.execute("SELECT count(*) FROM ecommerce.customers")
    branch_count = cur.fetchone()[0]  # 101

# Check production — unchanged
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM ecommerce.customers")
    prod_count = cur.fetchone()[0]  # Still 100!
```

The branch has 101 customers. Production still has 100. **Complete isolation, zero risk.**

### When to use this pattern
- **Development**: Query production data without risk
- **Testing**: Run integration tests against realistic data
- **Analytics**: Ad-hoc queries on a snapshot without affecting OLTP performance
- **Debugging**: Reproduce a production issue in an isolated environment

---

## Scenario 2: Schema Changes — Dev to Production

**The problem**: You need to add a `loyalty_tier` column to the `customers` table, backfill it with data, and push the change to production. How do you test it safely?

**The solution**: Develop on a branch, validate, then replay the migration on production.

```
production ─────────────────── replay migration ── production (with loyalty_tier)
       \                           ↑
        └── feature/loyalty-tier   │
             1. ALTER TABLE        │
             2. Backfill data      │
             3. Validate ──────────┘
             4. 🗑️ delete branch
```

### Step 1: Develop on a branch

We write the migration as **idempotent SQL** — this is important because we'll replay it later:

```sql
ALTER TABLE ecommerce.customers
ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'bronze';

UPDATE ecommerce.customers c
SET loyalty_tier = CASE
    WHEN order_count >= 5 THEN 'platinum'
    WHEN order_count >= 3 THEN 'gold'
    WHEN order_count >= 1 THEN 'silver'
    ELSE 'bronze'
END
FROM (
    SELECT customer_id, COUNT(*) as order_count
    FROM ecommerce.orders
    GROUP BY customer_id
) o
WHERE c.id = o.customer_id;
```

We apply this on the feature branch and validate — check tier distribution, verify no NULLs, confirm the column exists.

### Step 2: Visual schema comparison

Lakebase provides a built-in **Schema Diff** tool in the UI. Navigate to your feature branch and click "Schema diff" to see a visual comparison against the parent branch:

![Schema Comparison](https://github.com/CheeYuTan/lakebase_branching/blob/main/Compare_Schema.png?raw=true)

This makes it easy to review what changed before promoting — no need to write comparison queries manually.

> 📖 **Docs**: [Compare branch schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas)

### Step 3: Promote to production

Once validated, we replay the **exact same DDL** on production:

```python
# Same migration, now on production
with conn.cursor() as cur:
    cur.execute(MIGRATION_SQL)
```

Because the SQL uses `IF NOT EXISTS`, it's safe to run multiple times. The branch was our sandbox — production gets the battle-tested migration.

### The Migration Replay Pattern

```
1. Write idempotent DDL (ALTER TABLE ... IF NOT EXISTS, etc.)
2. Test on branch → validate → fix if needed → re-test
3. Once validated, replay the DDL on production
4. Delete the branch
```

This is the key workflow for schema changes with Lakebase branching. Branches are for **validation, not merging**. You develop and test your migration on a branch, then replay the same DDL on production.

---

## Scenario 3: Concurrent Changes — Production Drifted

**The problem**: You're developing a feature on a branch, but another team pushes schema changes to production while you're working. Your branch is now out of date.

**The solution**: Detect the drift, re-branch from the updated production, and re-test.

```
production ── another team adds email_verified ── replay both migrations ── production (final)
       \                                                ↑
        └── feature/order-priority                      │
             1. Add priority column                     │
             2. Discover drift!                         │
             3. Re-branch from updated production       │
             4. Re-test migration ──────────────────────┘
```

### The drift scenario

You create a branch and add a `priority` column to the `orders` table. Meanwhile, another team adds `email_verified` to `customers` on production. Your branch doesn't have that column.

### Detecting drift

Before promoting, compare schemas:

```python
# Production has: [id, name, email, created_at, loyalty_tier, email_verified]
# Your branch has: [id, name, email, created_at, loyalty_tier]

prod_only = set(prod_columns) - set(branch_columns)
# → {'email_verified'}  ⚠️ Production has drifted!
```

### The re-branch pattern

The fix is straightforward — create a **new branch** from the current production (which now includes the other team's changes), and re-apply your migration:

```python
# New branch from CURRENT production (has email_verified)
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)
    )),
    branch_id="feature-order-priority-v2"
).wait()

# Re-apply your migration — it's idempotent, safe to replay
with feature_conn_v2.cursor() as cur:
    cur.execute(YOUR_MIGRATION)
```

The new branch has both changes: `email_verified` from the other team, `priority` from yours. Validated and ready to promote.

### Key takeaways

1. **Always compare schemas** before promoting migrations
2. **Write idempotent DDL** so migrations can be replayed safely
3. **Re-branch from current production** when drift is detected — think of it like `git rebase`
4. **Branches are cheap** — creating a v2 costs nothing (copy-on-write)

---

## Scenario 4: CI/CD Ephemeral Branches

**The problem**: Every PR in your CI pipeline needs a real database to test against. Shared test databases cause flaky tests, and standing up fresh databases is slow and expensive.

**The solution**: Ephemeral branches with short TTLs. Each PR gets its own isolated database clone that auto-deletes.

```
production ────────────────────────────────────────── (untouched)
       ├── ci-pr-42  ── migrate ── test ── ✅ pass ── 🗑️ (TTL: 1h)
       ├── ci-pr-43  ── migrate ── test ── ❌ fail ── 🗑️ (TTL: 1h)
       └── ci-pr-44  ── migrate ── test ── ✅ pass ── 🗑️ (TTL: 1h)
```

### The pipeline simulation

We define three simulated pull requests, each with a migration and a test:

| PR | Migration | Expected Result |
|---|---|---|
| PR #42 | Add `loyalty_tier` column with default | ✅ Pass |
| PR #43 | Add `priority` column **without backfill** | ❌ Fail (test expects backfilled data) |
| PR #44 | Add `avg_rating` to products + backfill | ✅ Pass |

For each PR, we:
1. **Create a branch** with a 1-hour TTL
2. **Apply the migration**
3. **Run integration tests**
4. **Report results**

```python
for pr in PULL_REQUESTS:
    branch_name = f"ci-pr-{pr['pr_number']}"
    
    # Create ephemeral branch
    w.postgres.create_branch(
        parent=f"projects/{project_name}",
        branch=Branch(spec=BranchSpec(
            source_branch=prod_branch_name,
            ttl=Duration(seconds=3600)  # 1-hour TTL
        )),
        branch_id=branch_name
    ).wait()
    
    # Connect, migrate, test
    branch_conn, _, _ = connect_to_branch(branch_name)
    with branch_conn.cursor() as cur:
        cur.execute(pr['migration'])
        cur.execute(pr['test_query'])
        test_result = cur.fetchone()[0]
    
    passed = test_result > 0
    print(f"PR #{pr['pr_number']}: {'✅ PASSED' if passed else '❌ FAILED'}")
```

### The CI/CD report

```
═══════════════════════════════════════════════════════════════
  CI/CD TEST REPORT
═══════════════════════════════════════════════════════════════
  ✅ PR # 42 | Add customer loyalty tiers          | ci-pr-42
  ❌ PR # 43 | Add order priority (missing backfill)| ci-pr-43
  ✅ PR # 44 | Add product ratings                  | ci-pr-44

  Summary: 2 passed, 1 failed, 3 total
═══════════════════════════════════════════════════════════════
```

PR #43 correctly fails because the migration adds the column but doesn't backfill high-value orders — the test catches this gap. That's the whole point: **catch issues before they hit production**.

### Integrating with GitHub Actions

```yaml
steps:
  - name: Create test database
    run: |
      python create_branch.py \
        --branch "ci-pr-${{ github.event.number }}" \
        --ttl 3600
  - name: Run migrations
    run: python apply_migrations.py --branch "ci-pr-${{ github.event.number }}"
  - name: Run tests
    run: pytest --db-branch "ci-pr-${{ github.event.number }}"
```

No cleanup step needed. The TTL handles it.

### Why this matters

- **No shared test databases** — every PR gets its own isolated copy
- **No cleanup scripts** — TTL handles everything automatically
- **Instant provisioning** — copy-on-write means zero wait time
- **Cost-effective** — branches share storage until data diverges

---

## Branch Expiration: Set It and Forget It

Branch TTL (time-to-live) is one of those features that seems simple but changes how you work. Every branch can have an expiration:

| Use Case | Recommended TTL |
|---|---|
| CI/CD test runs | 1 hour |
| Feature development | 1–7 days |
| Staging environments | 7–30 days |
| Debugging sessions | 24 hours |

TTL is set at branch creation time:

```python
Branch(spec=BranchSpec(
    source_branch=prod_branch_name,
    ttl=Duration(seconds=3600)  # 1 hour
))
```

When the TTL expires, the branch and its compute endpoint are automatically deleted. No cron jobs, no cleanup scripts, no forgotten dev databases running up your bill.

---

## Authentication: No Passwords Required

One thing that surprised me about Lakebase is how clean the authentication story is. When you create a project, a Postgres role for your Databricks identity is **automatically created**. This role:

- Owns the default `databricks_postgres` database
- Is a member of `databricks_superuser`
- Authenticates via short-lived **OAuth tokens**

```python
# Generate a fresh OAuth token
cred = w.postgres.generate_database_credential(endpoint=endpoint_name)

# Connect with it
conn = psycopg2.connect(
    host=host,
    port=5432,
    dbname="databricks_postgres",
    user="your.email@company.com",
    password=cred.token,  # Short-lived OAuth token
    sslmode="require"
)
```

No passwords to rotate. No secrets to manage. Tokens auto-expire and are generated fresh each time.

---

## Try It Yourself

All four scenarios are available as self-contained Databricks notebooks:

| Notebook | Scenario |
|---|---|
| `00_Setup_Project` | Create project & seed data |
| `01_Scenario_Data_Only` | Production data in dev (read-only) |
| `02_Scenario_Schema_To_Prod` | Schema changes — dev to production |
| `03_Scenario_Concurrent` | Concurrent changes — handle drift |
| `04_Scenario_CICD_Ephemeral` | CI/CD ephemeral branches |
| `99_Cleanup` | Tear down everything |

**To reproduce:**
1. Clone the [GitHub repo](https://github.com/CheeYuTan/lakebase_branching)
2. Import the notebooks into your Databricks workspace (or sync via Git Folders)
3. Run `00_Setup_Project` first — it creates everything
4. Run any scenario notebook — each starts with `%run ./00_Setup_Project` for full independence

The project name is automatically derived from your Databricks username, so multiple users can run the demo simultaneously without conflicts.

---

## Key Takeaways

1. **Branching is instant and free.** Copy-on-write means no data duplication, no waiting, no storage overhead.

2. **Isolation is real.** Changes on a branch never leak to production. This is enforced at the storage level, not just by convention.

3. **The migration replay pattern works.** Write idempotent DDL, test on a branch, replay on production. Simple, safe, and auditable.

4. **TTL eliminates drift.** Forgotten dev branches don't pile up. Set a TTL and move on.

5. **CI/CD gets first-class database support.** Every PR can have its own isolated database clone with zero operational overhead.

Database branching isn't just a nice-to-have — it fundamentally changes how teams interact with their databases. The same way Git transformed collaborative software development, database branching removes the friction and fear from database development workflows.

---

*All code and notebooks are available on [GitHub](https://github.com/CheeYuTan/lakebase_branching). For more on Lakebase, check out the [official documentation](https://docs.databricks.com/aws/en/oltp/projects/manage-branches).*
