# Databricks Lakebase — Database Branching Demo

Hands-on notebooks that demonstrate **database branching** with [Databricks Lakebase](https://docs.databricks.com/aws/en/oltp/index.html). Each notebook walks through a real-world scenario using a simple e-commerce database.

## What Is Database Branching?

Database branching lets you create **instant, zero-cost copies** of your production database for development, testing, and CI/CD. Under the hood, Lakebase uses copy-on-write storage — branches share data with the parent until it diverges.

- **Instant** — branch creation takes less than a second
- **Isolated** — changes on a branch never affect production
- **Self-cleaning** — set a TTL and branches auto-delete

## Notebooks

| Notebook | Description |
|---|---|
| `00_Setup_Project` | Creates a Lakebase project, seeds an e-commerce schema with sample data, and defines helper functions used by all scenarios. |
| `01_Scenario_Data_Only` | Create a dev branch from production, query data safely, prove full isolation. |
| `02_Scenario_Schema_To_Prod` | Develop a schema change on a feature branch, validate it, then promote to production via migration replay. |
| `03_Scenario_Concurrent` | Handle production drift — detect that another team changed production while you were developing, re-branch, and re-test. |
| `04_Scenario_CICD_Ephemeral` | Simulate a CI/CD pipeline where each PR gets its own ephemeral branch with a short TTL. |
| `99_Cleanup` | Delete all branches and optionally the entire project. |

## Getting Started

### Prerequisites

- A Databricks workspace in a [supported region](https://docs.databricks.com/aws/en/oltp/index.html): `us-east-1`, `us-east-2`, `eu-central-1`, `eu-west-1`, `eu-west-2`, `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`
- Any Databricks cluster with Python 3.10+
- No passwords or secrets needed — authentication uses OAuth tokens from your Databricks identity

### How to Run

1. **Import notebooks** into your Databricks workspace:
   - Use [Git Folders](https://docs.databricks.com/en/repos/index.html) to sync this repo, **or**
   - Download and import the `notebooks/` folder manually

2. **Run `00_Setup_Project`** first — it creates the Lakebase project and seeds the data.

3. **Run any scenario notebook** — each one starts with `%run ./00_Setup_Project`, so they're fully independent. Run them in any order.

4. **Run `99_Cleanup`** when you're done to tear down branches and (optionally) the project.

> **Note**: The project name is automatically derived from your Databricks username (e.g. `lakebase-branching-jane-doe`), so multiple users can run the demo simultaneously without conflicts.

## E-Commerce Schema

All scenarios use this schema, seeded with realistic sample data:

```
ecommerce.customers   — 100 rows  (id, name, email, created_at)
ecommerce.products    —  50 rows  (id, name, price, category)
ecommerce.orders      — 200 rows  (id, customer_id, total, status, created_at)
ecommerce.order_items — 500 rows  (id, order_id, product_id, quantity, unit_price)
```

## Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/index.html)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Compare Branch Schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas)
- [Query with Python in Notebooks](https://docs.databricks.com/aws/en/oltp/projects/notebooks-python)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
