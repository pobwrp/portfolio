# DevOps & CI/CD Playbook

Curated automation snippets demonstrating how Databricks bundles, Airflow
deployments, and quality gates were wired into continuous delivery pipelines.
Each example is sanitised but reflects real production flows used at KKP.

## Contents
- `jenkins/databricks_bundle_pipeline.groovy` — Multi-stage Jenkins pipeline that linted,
  validated, deployed, and archived Databricks Asset Bundles.
- `jenkins/mwaa_sync_pipeline.groovy` — Jenkins pipeline pushing Airflow DAGs to AWS MWAA.
- `github-actions/airflow_mwaa_deploy.yml` — GitHub Actions workflow for packaging and
  syncing Airflow code to S3 with branch-based environments.

Update credential IDs, secret names, and paths to align with your infrastructure
before running the samples.
