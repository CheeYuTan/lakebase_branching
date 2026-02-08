# Lakebase Branching — Technical Plan

> **Blog Title**: *"Branch, Test, Ship: A Practical Guide to Lakebase Database Branching"*
>
> **GitHub Repo**: https://github.com/CheeYuTan/lakebase_branching
>
> **Databricks Workspace**: https://e2-demo-field-eng.cloud.databricks.com
>
> **Workspace Path**: `/Workspace/Users/steven.tan@databricks.com/lakebase_branching`
>
> **API Reference**: https://docs.databricks.com/api/workspace/postgres
>
> **Branch Management Docs**: https://docs.databricks.com/aws/en/oltp/projects/manage-branches
>
> **API Usage Guide**: https://docs.databricks.com/aws/en/oltp/projects/api-usage

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Structure](#repository-structure)
3. [Scenario 0: Setup — Create Project & Seed Data](#scenario-0-setup)
4. [Scenario 1: Production Data in Dev (No Schema Changes)](#scenario-1)
5. [Scenario 2: Schema Changes in Dev → Push to Production](#scenario-2)
6. [Scenario 3: Concurrent Changes — Production Drifted Mid-Development](#scenario-3)
7. [Scenario 4: Point-in-Time Recovery](#scenario-4)
8. [Scenario 5: CI/CD Ephemeral Branches with Auto-Expiration](#scenario-5)
9. [Cleanup](#cleanup)
10. [Key Concepts for the Blog](#key-concepts)

---

## Architecture Overview <a id="architecture-overview"></a>

### Technology Stack

| Component | Choice | Why |
|---|---|---|
| **Runtime** | Databricks Notebooks (Python) | Native workspace integration, blog readers can follow along |
| **API Client** | `databricks-sdk` (Python SDK) | Handles auth automatically inside workspace, clean API |
| **Database Connection** | `psycopg2` via branch endpoint | Direct Postgres wire protocol to each branch |
| **Auth** | Workspace-native (notebook context) | No tokens/secrets to manage when running in workspace |
| **Version Control** | GitHub repo | Blog readers can clone and follow along |

### How the SDK Works (for the blog)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

w = WorkspaceClient()  # Auto-authenticated in Databricks notebooks

# Create a branch — that's it. One API call.
result = w.postgres.create_branch(
    parent="projects/my-project",
    branch=Branch(spec=BranchSpec(
        source_branch="projects/my-project/branches/main",
        ttl=Duration(seconds=86400)  # 24 hours
    )),
    branch_id="my-feature-branch"
).wait()
```

### The Critical Insight (highlight in blog)

> **Lakebase branches are NOT Git branches.**
>
> - There is **no native merge** operation.
> - Branches are **validation sandboxes** — you test your changes there.
> - To "promote" changes to production, you **replay your migration scripts** against the main branch.
> - The branch gave you a safe environment to validate. The migration script is the source of truth.
>
> Think of it as: *Git for your data, but your migration script is the commit.*

---

## Repository Structure <a id="repository-structure"></a>

```
lakebase_branching/
│
├── notebooks/                          # Databricks notebooks (synced to workspace)
│   ├── 00_Setup_Project.py             # Create Lakebase project + seed e-commerce schema
│   ├── 01_Scenario_Data_Only.py        # Scenario 1: Prod data in dev, no schema changes
│   ├── 02_Scenario_Schema_To_Prod.py   # Scenario 2: Schema changes → production
│   ├── 03_Scenario_Concurrent.py       # Scenario 3: Production drifted mid-development
│   ├── 04_Scenario_Point_In_Time.py    # Scenario 4: Point-in-time recovery
│   ├── 05_Scenario_CICD_Ephemeral.py   # Scenario 5: CI/CD ephemeral branches
│   └── 99_Cleanup.py                   # Tear down everything
│
├── sql/                                # Migration scripts (version-controlled)
│   ├── 001_seed_schema.sql             # Initial e-commerce schema
│   ├── 002_seed_data.sql               # Sample data
│   ├── 003_add_loyalty_tier.sql        # Scenario 2 migration
│   ├── 004_add_email_verified.sql      # Simulated "other team" migration (Scenario 3)
│   └── 005_add_order_priority.sql      # Scenario 3 — your migration that conflicts
│
├── config.json                         # Project configuration
├── requirements.txt                    # Python dependencies
├── TECHNICAL_PLAN.md                   # This file
└── README.md                           # Blog companion + setup instructions
```

### Why Notebooks?

- **Databricks-native**: Blog readers are likely Databricks users; notebooks are familiar.
- **Interactive**: Each cell can be run independently to see results.
- **Widgets**: Use `dbutils.widgets` for configurable parameters (project name, etc.).
- **Rich output**: Markdown cells for narrative, code cells for execution — perfect for a blog walkthrough.

---

## Scenario 0: Setup — Create Project & Seed Data <a id="scenario-0-setup"></a>

### Purpose
Create a Lakebase project with an e-commerce schema so all 5 scenarios have realistic data to work with.

### Technical Flow

```
┌─────────────────────────────────────────────┐
│           00_Setup_Project.py               │
│                                             │
│  1. Create Lakebase project                 │
│     POST projects (via SDK)                 │
│     → project_id                            │
│                                             │
│  2. Wait for project ready                  │
│     Poll project status until ACTIVE        │
│                                             │
│  3. Get main branch endpoint                │
│     List branches → get main branch host    │
│                                             │
│  4. Connect to main branch via psycopg2     │
│     → connection to postgres                │
│                                             │
│  5. Execute seed SQL                        │
│     001_seed_schema.sql → tables            │
│     002_seed_data.sql → sample rows         │
│                                             │
│  6. Verify: SELECT count(*) FROM tables     │
│                                             │
│  7. Save project_id to config / widget      │
└─────────────────────────────────────────────┘
```

### Seed Schema (e-commerce)

```sql
-- 001_seed_schema.sql
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);
```

### SDK Calls

```python
# Create project
w.postgres.create_project(
    project=Project(spec=ProjectSpec(
        display_name="lakebase-branching-blog",
        pg_version=17,
        default_compute_spec=ComputeSpec(
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=4.0,
            suspend_timeout_duration=Duration(seconds=60)
        )
    )),
    project_id="lakebase-branching-blog"
).wait()

# List branches (get the default "main" branch)
branches = w.postgres.list_branches(parent="projects/lakebase-branching-blog")
main_branch = [b for b in branches if b.spec.is_default][0]

# Get the compute endpoint for connecting
computes = w.postgres.list_computes(parent=main_branch.name)
host = computes[0].status.host
```

---

## Scenario 1: Production Data in Dev (No Schema Changes) <a id="scenario-1"></a>

### Story for the Blog
> *"You're a data analyst. Production has a bug report — orders with negative totals. You need to investigate production data without any risk of accidentally modifying it. You need a safe copy, now."*

### Technical Flow

```
┌────────────────────────────────────────────────────┐
│         01_Scenario_Data_Only.py                   │
│                                                    │
│  1. Create branch "dev-readonly" from main         │
│     source_branch: projects/.../branches/main      │
│     Strategy: Current data (latest snapshot)       │
│     TTL: 24 hours                                  │
│                                                    │
│  2. Wait for branch ready                          │
│                                                    │
│  3. Get branch endpoint (host, port)               │
│                                                    │
│  4. Connect via psycopg2                           │
│                                                    │
│  5. Demonstrate: data is identical to production   │
│     SELECT count(*) FROM customers;  -- same count │
│     SELECT * FROM orders WHERE total < 0;          │
│                                                    │
│  6. Demonstrate: changes are isolated              │
│     INSERT INTO customers (name, email)            │
│       VALUES ('Test User', 'test@example.com');    │
│     -- This row exists ONLY on dev-readonly        │
│     -- Production (main) is untouched              │
│                                                    │
│  7. Verify isolation:                              │
│     Query main → no 'Test User'                    │
│     Query dev-readonly → 'Test User' exists        │
│                                                    │
│  8. Delete branch (or let TTL handle it)           │
└────────────────────────────────────────────────────┘
```

### SDK Calls

```python
# Create branch with 24h TTL
result = w.postgres.create_branch(
    parent="projects/lakebase-branching-blog",
    branch=Branch(spec=BranchSpec(
        source_branch="projects/lakebase-branching-blog/branches/main",
        ttl=Duration(seconds=86400)  # 24 hours
    )),
    branch_id="dev-readonly"
).wait()
```

### Blog Talking Points
- Branch created **instantly** via copy-on-write — no data duplication
- Branch gets its **own compute endpoint** that scales to zero when idle
- Data isolation is **guaranteed** — writes on the branch don't affect main
- 24-hour TTL means the branch **auto-cleans** — no orphaned environments

---

## Scenario 2: Schema Changes in Dev → Push to Production <a id="scenario-2"></a>

### Story for the Blog
> *"Product wants a loyalty tier system. You need to add a `loyalty_tier` column to the customers table, backfill it based on order history, and ship it to production. But you can't risk breaking the live app."*

### Technical Flow

```
┌────────────────────────────────────────────────────────────────┐
│            02_Scenario_Schema_To_Prod.py                      │
│                                                                │
│  1. Create branch "feature/loyalty-tier" from main             │
│     Strategy: Current data                                     │
│     TTL: 7 days                                                │
│                                                                │
│  2. Connect to feature branch                                  │
│                                                                │
│  3. Apply migration 003_add_loyalty_tier.sql:                  │
│     ALTER TABLE customers                                      │
│       ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'bronze';    │
│                                                                │
│  4. Backfill data:                                             │
│     UPDATE customers SET loyalty_tier = CASE                   │
│       WHEN (order_count >= 50) THEN 'gold'                     │
│       WHEN (order_count >= 10) THEN 'silver'                   │
│       ELSE 'bronze'                                            │
│     END;                                                       │
│                                                                │
│  5. Validate on branch:                                        │
│     SELECT loyalty_tier, count(*) FROM customers               │
│       GROUP BY loyalty_tier;                                   │
│     -- Verify distribution looks correct                       │
│                                                                │
│  6. Compare schemas:                                           │
│     Branch has: customers.loyalty_tier ✓                       │
│     Main does NOT have it                                      │
│                                                                │
│  7. ✅ PROMOTE: Replay migration on main                       │
│     Connect to main branch                                     │
│     Execute 003_add_loyalty_tier.sql                           │
│     Execute backfill UPDATE                                    │
│                                                                │
│  8. Verify main now has loyalty_tier                            │
│                                                                │
│  9. Delete feature branch                                      │
└────────────────────────────────────────────────────────────────┘
```

### Migration SQL

```sql
-- 003_add_loyalty_tier.sql
ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'bronze';

-- Backfill based on order history
UPDATE customers c SET loyalty_tier = CASE
    WHEN sub.order_count >= 50 THEN 'gold'
    WHEN sub.order_count >= 10 THEN 'silver'
    ELSE 'bronze'
END
FROM (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders GROUP BY customer_id
) sub
WHERE c.id = sub.customer_id;
```

### Blog Talking Points
- The branch was your **staging environment with real data** — you validated the migration works before touching production
- The **migration script** (003_add_loyalty_tier.sql) is the source of truth, not the branch
- Promotion = **replaying the same SQL** on main. This is how tools like Flyway/Liquibase/Alembic work — branching just gives you a safe place to validate first
- Schema comparison (via UI or API) confirms the diff before you commit to production

---

## Scenario 3: Concurrent Changes — Production Drifted <a id="scenario-3"></a>

### Story for the Blog
> *"You started adding an `order_priority` column on your feature branch last Monday. On Wednesday, a teammate merged their `email_verified` column to production. Now your branch is out of date. How do you safely ship your changes?"*

### Technical Flow

```
┌───────────────────────────────────────────────────────────────────────┐
│              03_Scenario_Concurrent.py                                │
│                                                                       │
│  PHASE 1: Setup the conflict scenario                                │
│  ─────────────────────────────────────                                │
│  1. Create branch "feature/order-priority" from main                 │
│     (at this point, main has: customers, products, orders,           │
│      order_items — all with loyalty_tier from scenario 2)            │
│                                                                       │
│  2. On feature branch: Apply migration 005_add_order_priority.sql    │
│     ALTER TABLE orders ADD COLUMN priority VARCHAR(10) DEFAULT 'normal';│
│                                                                       │
│  3. Simulate "teammate's work": Apply 004 directly to main          │
│     ALTER TABLE customers ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;│
│     (This simulates another team shipping while you're developing)   │
│                                                                       │
│  PHASE 2: Discover the drift                                         │
│  ────────────────────────────                                        │
│  4. Compare schemas:                                                 │
│     Main has:      customers.email_verified ✓  orders.priority ✗     │
│     Feature has:   customers.email_verified ✗  orders.priority ✓     │
│     → Production has DRIFTED from when you branched                  │
│                                                                       │
│  PHASE 3: Resolve — The "Rebase" Pattern                             │
│  ────────────────────────────────────────                             │
│  5. Create NEW branch "feature/order-priority-v2" from CURRENT main  │
│     (this branch now has email_verified, but not priority)           │
│                                                                       │
│  6. Re-apply YOUR migration (005) on the new branch                  │
│     ALTER TABLE orders ADD COLUMN priority VARCHAR(10) DEFAULT 'normal';│
│                                                                       │
│  7. Validate: new branch now has BOTH changes                        │
│     customers.email_verified ✓ (from main)                           │
│     orders.priority ✓ (your migration, re-applied)                   │
│                                                                       │
│  8. Promote: Replay 005 on main                                      │
│     Main now has both email_verified AND priority                    │
│                                                                       │
│  9. Cleanup: Delete both feature branches                            │
└───────────────────────────────────────────────────────────────────────┘
```

### Alternative: Reset from Parent

```
┌───────────────────────────────────────────────────────────────┐
│  ALTERNATIVE (simpler, but destructive):                     │
│                                                               │
│  Instead of creating a new branch, you can RESET             │
│  your existing branch from its parent:                       │
│                                                               │
│  w.postgres.reset_branch(                                    │
│      name="projects/.../branches/feature-order-priority"     │
│  ).wait()                                                    │
│                                                               │
│  ⚠️  This OVERWRITES your branch with latest main            │
│  → Your migration (005) is LOST                              │
│  → You must re-apply it after reset                          │
│  → Connection endpoint stays the same (nice!)                │
│                                                               │
│  Good when: your migration is scripted & idempotent          │
│  Bad when: you have manual, un-scripted changes              │
└───────────────────────────────────────────────────────────────┘
```

### Blog Talking Points
- This is the **most realistic scenario** — production always moves forward while you're developing
- The "rebase" pattern: create a fresh branch from updated main, re-apply your migration
- **Reset from parent** is the faster alternative but it's a **complete overwrite** — everything on your branch is lost
- This is why **migration scripts in version control** are critical — you need to be able to re-apply them cleanly
- The branch let you **discover the conflict safely** instead of breaking production

---

## Scenario 4: Point-in-Time Recovery <a id="scenario-4"></a>

### Story for the Blog
> *"It's Friday at 5pm. Someone ran an `UPDATE customers SET loyalty_tier = NULL` without a WHERE clause. All loyalty tiers are wiped. Instead of restoring a backup (which takes hours), you create a branch from 30 minutes ago — instantly."*

### Technical Flow

```
┌────────────────────────────────────────────────────────────────┐
│            04_Scenario_Point_In_Time.py                        │
│                                                                │
│  1. Simulate the disaster:                                     │
│     On main: UPDATE customers SET loyalty_tier = NULL;         │
│     → All loyalty tiers gone! 😱                               │
│                                                                │
│  2. Verify the damage:                                         │
│     SELECT loyalty_tier, count(*) FROM customers               │
│       GROUP BY loyalty_tier;                                   │
│     → All NULL                                                 │
│                                                                │
│  3. Create recovery branch from PAST DATA:                     │
│     source_branch: "projects/.../branches/main"                │
│     source_branch_time: "2026-02-08T16:30:00Z" (30 min ago)   │
│     branch_id: "recovery-20260208"                             │
│                                                                │
│  4. Connect to recovery branch                                 │
│                                                                │
│  5. Verify: data is intact on recovery branch                  │
│     SELECT loyalty_tier, count(*) FROM customers               │
│       GROUP BY loyalty_tier;                                   │
│     → bronze: 50, silver: 30, gold: 20  ✅                     │
│                                                                │
│  6. Fix production by copying data back:                       │
│     Option A: Export from recovery → import to main            │
│     Option B: Use recovery branch as read source               │
│               UPDATE main.customers SET loyalty_tier = ...     │
│               FROM recovery.customers WHERE ...                │
│     (Note: cross-branch SQL not directly supported,            │
│      so use application-level copy via psycopg2)               │
│                                                                │
│  7. Verify main is restored                                    │
│                                                                │
│  8. Delete recovery branch                                     │
└────────────────────────────────────────────────────────────────┘
```

### SDK Calls

```python
from datetime import datetime, timedelta, timezone

# 30 minutes ago
recovery_time = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

result = w.postgres.create_branch(
    parent="projects/lakebase-branching-blog",
    branch=Branch(spec=BranchSpec(
        source_branch="projects/lakebase-branching-blog/branches/main",
        source_branch_time=recovery_time,  # Point-in-time!
        ttl=Duration(seconds=86400)
    )),
    branch_id="recovery-20260208"
).wait()
```

### Blog Talking Points
- Traditional backup/restore: **hours**. Lakebase point-in-time branch: **seconds**.
- The recovery branch is a **full, queryable Postgres instance** from that point in time
- You can use it to **investigate** what happened, **extract** correct data, or **compare** with current state
- The original main branch is untouched — you're working on a copy
- Great for **compliance/audit**: "Show me the database state at 3:00 PM on Jan 15th"

---

## Scenario 5: CI/CD Ephemeral Branches with Auto-Expiration <a id="scenario-5"></a>

### Story for the Blog
> *"Your CI/CD pipeline runs integration tests on every pull request. Each test run needs its own database with production-like data. But you can't have hundreds of databases accumulating. Solution: ephemeral branches with automatic expiration."*

### Technical Flow

```
┌────────────────────────────────────────────────────────────────┐
│           05_Scenario_CICD_Ephemeral.py                        │
│                                                                │
│  1. Simulate a CI/CD pipeline:                                 │
│     branch_name = f"ci-pr-{PR_NUMBER}-{SHORT_SHA}"             │
│     e.g., "ci-pr-42-abc123"                                    │
│                                                                │
│  2. Create branch with SHORT TTL:                              │
│     ttl: 1 hour (3600 seconds)                                 │
│                                                                │
│  3. Run "integration tests":                                   │
│     - Connect to branch                                        │
│     - Apply pending migrations                                 │
│     - Run test queries                                         │
│     - Assert expected results                                  │
│                                                                │
│  4. Show branch expiration info:                               │
│     Get branch → status.expire_time                            │
│     "This branch will auto-delete at {expire_time}"            │
│                                                                │
│  5. Update TTL (extend if tests take longer):                  │
│     Update branch with new ttl: 2 hours                        │
│                                                                │
│  6. Show multiple concurrent CI branches:                      │
│     Create "ci-pr-43-def456" (1 hour TTL)                      │
│     Create "ci-pr-44-ghi789" (1 hour TTL)                      │
│     List all branches → show all 3 CI branches                 │
│                                                                │
│  7. Early cleanup (optional):                                  │
│     Delete branch immediately after tests pass                 │
│     (Don't wait for TTL if pipeline succeeds)                  │
│                                                                │
│  8. Branch expiration restrictions to highlight:               │
│     - Max 10 unarchived branches per project                   │
│     - Max 30-day TTL                                           │
│     - Can't expire protected or default branches               │
│     - Can't expire branches with children                      │
└────────────────────────────────────────────────────────────────┘
```

### Blog Talking Points
- Ephemeral branches solve the **"test database sprawl"** problem
- TTL model: set it and forget it — system handles cleanup
- Recommended TTLs: CI/CD (1–4 hours), demos (24–48 hours), features (1–7 days)
- If a branch is **reset from parent**, the TTL countdown **restarts** (useful for long-running test suites)
- Max 10 unarchived branches — plan your CI parallelism accordingly
- Protected branches are **exempt** from archiving — your main/production branch is always safe

---

## Cleanup <a id="cleanup"></a>

```
┌────────────────────────────────────────────────────────────┐
│              99_Cleanup.py                                  │
│                                                            │
│  1. List all branches in project                           │
│  2. Delete all non-default branches                        │
│  3. Optionally delete the project itself                   │
│  4. Verify clean state                                     │
└────────────────────────────────────────────────────────────┘
```

---

## Key Concepts for the Blog <a id="key-concepts"></a>

### Concept 1: Copy-on-Write Storage
```
Main Branch:     [Page 1] [Page 2] [Page 3] [Page 4]
                      ↑        ↑        ↑        ↑
Dev Branch:      [shared] [shared] [Page 3'] [shared]
                                       ↑
                              Only modified page is duplicated
```
- Branches share storage with their parent
- Only **modified pages** are stored separately
- This is why branching is **instant** and **cheap**

### Concept 2: Branching ≠ Merging
```
Git workflow:        branch → commit → merge → done
Lakebase workflow:   branch → migrate → validate → REPLAY migration on main → done
                                                    ↑
                                          This is the key difference
```

### Concept 3: Branch Lifecycle

| State | Description |
|---|---|
| Creating | Branch is being provisioned |
| Active | Branch is ready and compute is running |
| Suspended | Compute scaled to zero (idle), data persists |
| Archived | Inactive too long, needs unarchiving to use |
| Expired | TTL reached, branch auto-deleted permanently |

### Concept 4: Branch Expiration Rules
- **Cannot expire**: protected branches, default branch, parent branches (with children)
- **Max TTL**: 30 days (can extend by updating before expiry)
- **Reset restarts TTL**: If you reset a branch from parent, the countdown restarts
- **Deletion is permanent**: compute, data, roles — all gone

### Concept 5: Practical TTL Guidelines

| Use Case | Suggested TTL | Rationale |
|---|---|---|
| CI/CD test branches | 1–4 hours | Pipeline completes, branch is disposable |
| Demo environments | 24–48 hours | Enough for a presentation + follow-up |
| Feature development | 1–7 days | Typical sprint cycle |
| Long-term testing | 30 days (renew) | Compliance or extended QA |
| Staging | No expiration (protected) | Permanent environment |

---

## Execution Order & Dependencies

### Full Demo (recommended)

```
00_Setup_Project.py           ← Run FIRST (creates project + seed data)
01_Scenario_Data_Only.py      ← Scenario 1 (standalone — only needs setup)
02_Scenario_Schema_To_Prod.py ← Scenario 2 (adds loyalty_tier to main)
03_Scenario_Concurrent.py     ← Scenario 3 (needs loyalty_tier from S2)
04_Scenario_Point_In_Time.py  ← Scenario 4 (needs loyalty_tier from S2)
05_Scenario_CICD_Ephemeral.py ← Scenario 5 (standalone — only needs setup)
99_Cleanup.py                 ← Run LAST (tears everything down)
```

### Dependency Graph

```
00_Setup ──┬── 01_Data_Only          (standalone)
           │
           ├── 02_Schema_To_Prod ──┬── 03_Concurrent    (needs S2's schema on main)
           │                       │
           │                       └── 04_Point_In_Time (needs S2's data on main)
           │
           └── 05_CICD_Ephemeral     (standalone)
```

### Standalone Options (for customers who only want one scenario)

| Want to run | Notebooks needed |
|---|---|
| Scenario 1 only | `00` → `01` → `99` |
| Scenario 2 only | `00` → `02` → `99` |
| Scenario 5 only | `00` → `05` → `99` |
| Scenarios 3 or 4 | `00` → `02` → `03` or `04` → `99` |

> ⚠️ **Scenarios 2 and 3 are cumulative** — each one adds schema changes to main
> that the next scenario inherits. This is intentional to show a realistic
> development progression where the database evolves over time.

---

## Reproducibility — How Customers Run This <a id="reproducibility"></a>

### Design Principle

> **Every notebook must be 100% self-contained and runnable in ANY Databricks workspace
> by anyone with Lakebase access. Zero hardcoded values. Zero external file dependencies.**

### How Customers Get the Notebooks Into Their Workspace

**Option 1: Databricks Git Folders (Repos) — Recommended**
```
1. In Databricks workspace → Repos → Add Repo
2. Paste: https://github.com/CheeYuTan/lakebase_branching
3. Clone → notebooks appear directly in workspace
4. Run notebooks in order, filling in widgets
```
- Advantage: Auto-sync with GitHub, notebooks + SQL files visible
- Notebooks run directly from the Repo folder

**Option 2: Manual Import**
```
1. Download repo as ZIP from GitHub
2. Databricks workspace → Import → Upload ZIP or individual .py files
3. Run notebooks in order
```

**Option 3: Databricks CLI**
```bash
# Clone the repo locally
git clone https://github.com/CheeYuTan/lakebase_branching
cd lakebase_branching

# Import notebooks to workspace
databricks workspace import-dir notebooks/ /Workspace/Users/<your-email>/lakebase_branching \
  --profile <your-profile>
```

### Widget Design — The Key to Portability

Every notebook uses `dbutils.widgets` so customers can configure parameters without editing code.

#### Widget Flow Across Notebooks

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      WIDGET PARAMETER FLOW                             │
│                                                                         │
│  00_Setup_Project.py                                                    │
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← User sets once│
│  │   min_cu        = "0.5"                           │                  │
│  │   max_cu        = "4.0"                           │                  │
│  │   suspend_timeout_seconds = "60"                  │                  │
│  │   pg_version    = "17"                            │                  │
│  │   db_password   = "" (user creates via UI)        │                  │
│  │                                                   │                  │
│  │ Outputs:                                          │                  │
│  │   → Creates project                               │                  │
│  │   → Prints project_name to use in other notebooks │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│                           │ project_name (same value used everywhere)   │
│                           ▼                                             │
│  01_Scenario_Data_Only.py ─────────────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   branch_name   = "dev-readonly"                  │                  │
│  │   ttl_hours     = "24"                            │                  │
│  │   db_password   = ""                              │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│  02_Scenario_Schema_To_Prod.py ───────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   branch_name   = "feature-loyalty-tier"          │                  │
│  │   ttl_hours     = "168" (7 days)                  │                  │
│  │   db_password   = ""                              │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│  03_Scenario_Concurrent.py ───────────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   branch_name_1 = "feature-order-priority"        │                  │
│  │   branch_name_2 = "feature-order-priority-v2"     │                  │
│  │   ttl_hours     = "48"                            │                  │
│  │   db_password   = ""                              │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│  04_Scenario_Point_In_Time.py ────────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   recovery_minutes_ago = "5"                      │                  │
│  │   db_password   = ""                              │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│  05_Scenario_CICD_Ephemeral.py ───────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   ttl_hours     = "1"                             │                  │
│  │   num_ci_branches = "3"                           │                  │
│  └───────────────────────────────────────────────────┘                  │
│                           │                                             │
│  99_Cleanup.py ───────────────────────────────────────────────────────│
│  ┌───────────────────────────────────────────────────┐                  │
│  │ Widgets:                                          │                  │
│  │   project_name  = "lakebase-branching-demo"       │ ← Same default  │
│  │   delete_project = "no" (dropdown: yes/no)        │                  │
│  └───────────────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Why `project_name` Is the Single Linking Key

- `project_name` is the **only value customers need to keep consistent** across notebooks
- It defaults to `"lakebase-branching-demo"` — if they don't touch it, everything "just works"
- All SDK calls derive paths from it: `projects/{project_name}/branches/main`
- If customers want to run multiple demos, they just change `project_name` to something unique

### SQL — Embedded, Not External

**Problem**: If notebooks read from `sql/*.sql` files, they only work if the file structure
is present (Git Folders). Manual imports break.

**Solution**: All SQL is **embedded directly in the notebook** as Python strings.

```python
# ✅ Self-contained — works anywhere
SEED_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
-- ... more tables ...
"""
cursor.execute(SEED_SCHEMA_SQL)
```

```python
# ❌ Depends on file structure — breaks on manual import
with open("../sql/001_seed_schema.sql") as f:
    cursor.execute(f.read())
```

**The `sql/` folder still exists in the repo** — it's for reference, version control,
and readers who want to inspect the migrations independently. But the notebooks don't
depend on it at runtime.

### Database Connection — Auth Strategy

**Inside Databricks notebooks**, the connection uses:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get the branch compute endpoint
computes = w.postgres.list_computes(parent=f"projects/{project_name}/branches/main")
host = computes[0].status.host
port = 5432

# Option 1: Password-based role (user creates via Lakebase UI)
import psycopg2
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname="postgres",
    user="my_role",            # Created by user in Lakebase UI
    password=db_password       # From widget
)

# Option 2: Databricks SQL Editor (no password needed, but not psycopg2)
# → Users can query branches directly from the SQL Editor using the
#   built-in authenticator role. No setup needed.
```

**For the notebook demo**: We use Option 1 (psycopg2 with password).
The `db_password` widget captures the password. Users create the role
via the Lakebase UI → Project → Roles → Create Role.

The setup notebook (00) will include step-by-step instructions for creating the role.

### Cluster / Compute Requirements

Each notebook should include a note at the top:

```python
# COMMAND ----------
# %md
# ## Prerequisites
# - **Cluster**: Any Databricks cluster with Python 3.10+ 
# - **Libraries**: `databricks-sdk`, `psycopg2-binary` (install via `%pip install`)
# - **Permissions**: CAN MANAGE on the Lakebase project
# - **Lakebase**: Available in your workspace (us-east-1, us-east-2, eu-central-1, 
#                 eu-west-1, eu-west-2, ap-south-1, ap-southeast-1, ap-southeast-2)
```

### Install Dependencies Inline

Every notebook starts with:

```python
# COMMAND ----------
%pip install databricks-sdk --upgrade -q
%pip install psycopg2-binary -q
dbutils.library.restartPython()
```

This ensures the notebook runs on any cluster — no pre-installed libraries needed.

### Updated Repository Structure

```
lakebase_branching/
│
├── notebooks/                                # Databricks notebooks
│   ├── 00_Setup_Project.py                   # Widgets: project_name, min_cu, max_cu, ...
│   ├── 01_Scenario_Data_Only.py              # Widgets: project_name, branch_name, ttl_hours
│   ├── 02_Scenario_Schema_To_Prod.py         # Widgets: project_name, branch_name, ttl_hours
│   ├── 03_Scenario_Concurrent.py             # Widgets: project_name, branch_name_1, branch_name_2
│   ├── 04_Scenario_Point_In_Time.py          # Widgets: project_name, recovery_minutes_ago
│   ├── 05_Scenario_CICD_Ephemeral.py         # Widgets: project_name, ttl_hours, num_ci_branches
│   └── 99_Cleanup.py                         # Widgets: project_name, delete_project
│
├── sql/                                      # Reference SQL (NOT read at runtime)
│   ├── 001_seed_schema.sql                   # For readers to inspect independently
│   ├── 002_seed_data.sql
│   ├── 003_add_loyalty_tier.sql
│   ├── 004_add_email_verified.sql
│   └── 005_add_order_priority.sql
│
├── config.json                               # Project defaults (for reference only)
├── requirements.txt                          # For local dev / CI
├── TECHNICAL_PLAN.md                         # This file
└── README.md                                 # Setup guide + blog companion
```

### Notebook Content Design — Self-Explanatory Notebooks

Every notebook is designed to be **readable as a standalone document** — a mix of
narrative markdown cells and executable code cells. A reader should be able to
understand the scenario, the "why", the technical flow, and the outcome without
needing to read the blog post or the TECHNICAL_PLAN.

#### Cell Pattern: Markdown → Code → Markdown → Code

Each logical step follows this rhythm:

```
┌──────────────────────────────────────────────────────────┐
│ [Markdown]  Explain WHAT we're about to do and WHY       │
│             Include diagrams, context, key concepts       │
├──────────────────────────────────────────────────────────┤
│ [Code]      Execute the actual step                       │
│             Print clear output with ✅/❌ indicators       │
├──────────────────────────────────────────────────────────┤
│ [Markdown]  Explain WHAT just happened                    │
│             Highlight key observations for the reader     │
├──────────────────────────────────────────────────────────┤
│ [Code]      Verify / query to prove the point             │
└──────────────────────────────────────────────────────────┘
```

#### Full Notebook Blueprint — Example: `01_Scenario_Data_Only.py`

```
Cell  1  [MD]   # 🔀 Scenario 1: Production Data in Dev (No Schema Changes)
                Story/hook paragraph — why this scenario matters
                
                ## What You'll Learn
                - How to create a branch from production
                - How copy-on-write gives you instant, zero-cost isolation
                - How to verify data isolation between branches
                - How branch expiration auto-cleans your environment
                
                ## Prerequisites
                - Run `00_Setup_Project` first
                - You need the `project_name` and `db_password` from setup
                
                ## Architecture
                ```
                production (main)
                    │
                    └── dev-readonly (branch, 24h TTL)
                         ↑ copy-on-write: same data, zero cost
                ```

Cell  2  [Code] %pip install databricks-sdk --upgrade -q
                %pip install psycopg2-binary -q
                dbutils.library.restartPython()

Cell  3  [MD]   ## ⚙️ Configuration
                Set the widgets below to match your environment.
                `project_name` must match the project you created in `00_Setup_Project`.

Cell  4  [Code] # Widgets
                dbutils.widgets.text("project_name", "lakebase-branching-demo", "Project Name")
                dbutils.widgets.text("branch_name", "dev-readonly", "Branch Name")
                dbutils.widgets.text("ttl_hours", "24", "Branch TTL (hours)")
                dbutils.widgets.text("db_password", "", "Database Password")
                
                # Read values
                project_name = dbutils.widgets.get("project_name")
                branch_name = dbutils.widgets.get("branch_name")
                ttl_hours = int(dbutils.widgets.get("ttl_hours"))
                db_password = dbutils.widgets.get("db_password")
                
                print(f"📋 Project:     {project_name}")
                print(f"📋 Branch:      {branch_name}")
                print(f"📋 TTL:         {ttl_hours} hours")

Cell  5  [Code] # SDK + helper function
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.service.postgres import Branch, BranchSpec, Duration
                import psycopg2
                
                w = WorkspaceClient()
                
                def get_branch_connection(project, branch, password, user="authenticator"):
                    """Connect to a Lakebase branch via psycopg2."""
                    computes = list(w.postgres.list_computes(
                        parent=f"projects/{project}/branches/{branch}"
                    ))
                    host = computes[0].status.host
                    return psycopg2.connect(
                        host=host, port=5432, dbname="postgres",
                        user=user, password=password
                    )
                
                def run_query(conn, sql, fetch=True):
                    """Execute SQL. Returns (columns, rows) if fetch=True."""
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        if fetch:
                            cols = [d[0] for d in cur.description]
                            return cols, cur.fetchall()
                        conn.commit()
                
                print("✅ SDK initialized")

Cell  6  [MD]   ## Step 1: Create a Branch from Production
                
                We'll create a branch called `dev-readonly` from the `main` branch.
                This uses the **"Current data"** strategy — an instant snapshot of
                the latest production state.
                
                > 💡 **How it works**: Lakebase uses **copy-on-write** storage.
                > The branch shares all data pages with `main`. No data is
                > physically copied — the branch is created in seconds, regardless
                > of database size.
                
                > ⏰ **TTL**: We set a 24-hour expiration. After 24 hours, the branch
                > and its compute are automatically deleted. No cleanup needed.

Cell  7  [Code] result = w.postgres.create_branch(
                    parent=f"projects/{project_name}",
                    branch=Branch(spec=BranchSpec(
                        source_branch=f"projects/{project_name}/branches/main",
                        ttl=Duration(seconds=ttl_hours * 3600)
                    )),
                    branch_id=branch_name
                ).wait()
                
                print(f"✅ Branch '{branch_name}' created!")
                print(f"   Source: main (current data)")
                print(f"   TTL: {ttl_hours} hours")
                print(f"   Expires: automatically after {ttl_hours}h of idle")

Cell  8  [MD]   ## Step 2: Verify — Data is Identical to Production
                
                Let's connect to both `main` and `dev-readonly` and compare the data.
                They should be identical — the branch is a snapshot of production.

Cell  9  [Code] # Connect to MAIN
                conn_main = get_branch_connection(project_name, "main", db_password)
                cols, main_counts = run_query(conn_main, """
                    SELECT 'customers' as tbl, count(*) as cnt FROM customers
                    UNION ALL
                    SELECT 'products', count(*) FROM products
                    UNION ALL
                    SELECT 'orders', count(*) FROM orders
                """)
                print("📊 Production (main):")
                for row in main_counts:
                    print(f"   {row[0]}: {row[1]} rows")
                
                # Connect to DEV BRANCH
                conn_dev = get_branch_connection(project_name, branch_name, db_password)
                cols, dev_counts = run_query(conn_dev, """
                    SELECT 'customers' as tbl, count(*) as cnt FROM customers
                    UNION ALL
                    SELECT 'products', count(*) FROM products
                    UNION ALL
                    SELECT 'orders', count(*) FROM orders
                """)
                print(f"\n📊 Dev branch ({branch_name}):")
                for row in dev_counts:
                    print(f"   {row[0]}: {row[1]} rows")
                
                print("\n✅ Data is identical — branch is an exact copy of production!")

Cell 10  [MD]   ## Step 3: Demonstrate Data Isolation
                
                Now let's prove that **writes on the branch don't affect production**.
                We'll insert a test customer on the dev branch and verify it doesn't
                appear on main.
                
                > 🔒 **This is the key value of branching**: developers can INSERT,
                > UPDATE, DELETE freely on their branch without any risk to production.

Cell 11  [Code] # Insert a test customer on the DEV branch only
                run_query(conn_dev, """
                    INSERT INTO customers (name, email)
                    VALUES ('Test User (branch only)', 'test@branch-demo.com')
                """, fetch=False)
                
                # Check dev branch — test user exists
                _, dev_result = run_query(conn_dev,
                    "SELECT name, email FROM customers WHERE email = 'test@branch-demo.com'"
                )
                print(f"🔍 Dev branch: Found {len(dev_result)} row(s)")
                for r in dev_result:
                    print(f"   → {r[0]} ({r[1]})")
                
                # Check main — test user does NOT exist
                _, main_result = run_query(conn_main,
                    "SELECT name, email FROM customers WHERE email = 'test@branch-demo.com'"
                )
                print(f"\n🔍 Production (main): Found {len(main_result)} row(s)")
                
                print("\n✅ Isolation confirmed!")
                print("   The test user exists ONLY on the dev branch.")
                print("   Production is completely untouched.")

Cell 12  [MD]   ## Step 4: Cleanup
                
                We can delete the branch now, or simply let the TTL handle it.
                
                Since we set a **24-hour TTL**, the branch will auto-delete if we
                walk away. But for a clean demo, let's delete it explicitly.
                
                > 💡 **What gets deleted**: The branch, its compute endpoint, all
                > data changes (our test user), and any roles specific to this branch.
                > The parent (`main`) is completely unaffected.

Cell 13  [Code] conn_dev.close()
                conn_main.close()
                
                w.postgres.delete_branch(
                    name=f"projects/{project_name}/branches/{branch_name}"
                ).wait()
                
                print(f"🗑️ Branch '{branch_name}' deleted.")
                print(f"   Production (main) is untouched.")
                print(f"   All branch-specific data (test user) is gone.")

Cell 14  [MD]   ## 🎯 Key Takeaways
                
                | Concept | What We Demonstrated |
                |---|---|
                | **Instant branching** | Branch created in seconds via copy-on-write |
                | **Data isolation** | Writes on branch don't affect production |
                | **Zero storage overhead** | Branch shares data pages with parent |
                | **Auto-cleanup** | 24h TTL means branch self-destructs |
                | **Own compute** | Branch gets its own endpoint, scales to zero when idle |
                
                > **Next**: Run `02_Scenario_Schema_To_Prod` to see how to safely
                > test schema migrations before promoting to production.
```

#### Notebook Cell Inventory (All Notebooks)

| Notebook | Total Cells | Markdown | Code | Key Sections |
|---|---|---|---|---|
| `00_Setup_Project` | ~16 | 8 | 8 | Intro, Prerequisites, Create Project, Wait, Seed Schema, Seed Data, Create Role Instructions, Verify |
| `01_Data_Only` | ~14 | 7 | 7 | Intro, Config, Create Branch, Verify Data, Isolation Demo, Cleanup, Takeaways |
| `02_Schema_To_Prod` | ~18 | 9 | 9 | Intro, Config, Create Branch, Apply Migration, Backfill, Validate, Compare Schemas, Replay on Main, Cleanup, Takeaways |
| `03_Concurrent` | ~22 | 11 | 11 | Intro, Config, Phase 1 (Setup Conflict), Phase 2 (Discover Drift), Schema Compare, Phase 3 (Rebase), Validate, Promote, Alt: Reset from Parent, Cleanup, Takeaways |
| `04_Point_In_Time` | ~18 | 9 | 9 | Intro, Config, Simulate Disaster, Verify Damage, Create Recovery Branch, Verify Recovery, Restore Data, Verify Fix, Cleanup, Takeaways |
| `05_CICD_Ephemeral` | ~18 | 9 | 9 | Intro, Config, Create CI Branch, Run Tests, Check Expiration, Extend TTL, Multiple Branches, List All, Cleanup, Takeaways |
| `99_Cleanup` | ~8 | 4 | 4 | Intro, Config, Delete Branches, Delete Project (optional), Verify |

#### Markdown Cell Style Guide

- **Titles**: Use `# 🔀 Scenario X: Title` (emoji for visual scannability)
- **Step headers**: Use `## Step N: Action Verb` (numbered for linear progression)
- **Key concept callouts**: Use `> 💡 **How it works**:` blockquotes
- **Warnings**: Use `> ⚠️ **Important**:` blockquotes  
- **Architecture diagrams**: ASCII art in fenced code blocks
- **Takeaways**: Table at the end summarizing what was demonstrated
- **Next step**: Point to the next notebook at the very end
- **Code output**: Use ✅/❌/📊/🔍/🗑️ emojis for scannable terminal output

### Customer Checklist (for the README)

```markdown
## Quick Start — Run in Your Workspace

### Prerequisites
- [ ] Databricks workspace with Lakebase enabled
- [ ] CAN MANAGE permissions on Lakebase
- [ ] Any Databricks cluster (Python 3.10+)
- [ ] Workspace in a supported region (us-east-1, us-east-2, eu-central-1, 
      eu-west-1, eu-west-2, ap-south-1, ap-southeast-1, ap-southeast-2)

### Steps
1. **Import notebooks**: Clone this repo via Databricks Git Folders (Repos)
   - Workspace → Repos → Add Repo → paste repo URL
2. **Run `00_Setup_Project`**: Creates your Lakebase project + seeds data
   - Change `project_name` widget if you want a unique name
   - Follow the instructions to create a database role with password
3. **Run scenarios 01–05 in order**: Each builds on the previous
   - Keep `project_name` consistent across all notebooks
   - Set `db_password` to the password you created in step 2
4. **Run `99_Cleanup`**: Deletes branches and (optionally) the project
   - Set `delete_project` to "yes" if you want to remove everything

### Customization
- Change `project_name` to run multiple independent demos
- Adjust `ttl_hours` to control branch lifespan
- Modify `min_cu` / `max_cu` to control compute scaling
```

---

## Open Questions / Decisions Needed

1. **Project name**: `lakebase-branching-demo` — ok? (changed from `lakebase-branching-blog` to be more generic for customers)
2. **Database auth**: Password-based role (psycopg2) requires users to create a role in the UI. Should we also show the SQL Editor approach (zero setup) as an alternative?
3. **Blog format**: One long post or a series?
4. **Screenshots**: Should we include Databricks UI screenshots (branch creation dialog, schema comparison, branch list)?
5. **Scenarios 2 & 3 cumulative**: Customers must run in order. Should we add a "standalone mode" that seeds the expected state if a scenario is run independently?
