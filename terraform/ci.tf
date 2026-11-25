# --- CI: GitHub Actions -> GCP without a JSON key (Workload Identity Federation) ---------------
#
# GitHub issues each workflow run a short-lived OIDC token. The pool below trusts tokens from
# GitHub, but only for var.github_repository, and lets them impersonate the CI service account
# for that run. No long-lived key exists anywhere, so there is nothing to leak or rotate.
#
# Everything here is skipped when github_repository is empty (forks without CI set up).
# The CI service account can only: run BigQuery jobs, write oss_pulse_ci, read oss_pulse_raw,
# read prod staging/marts (dbt --defer) and read gs://<lake>/dbt/prod/ (the prod manifest).

locals {
  ci_enabled = var.github_repository != ""
}

resource "google_bigquery_dataset" "ci" {
  count = local.ci_enabled ? 1 : 0

  dataset_id                  = var.bq_dataset_ci
  description                 = "dbt CI builds (GitHub Actions). Disposable: tables expire after 1 day."
  location                    = var.region
  default_table_expiration_ms = 24 * 60 * 60 * 1000
  delete_contents_on_destroy  = true # only CI scratch tables live here
  labels                      = merge(local.labels, { layer = "ci" })

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "ci" {
  count = local.ci_enabled ? 1 : 0

  account_id   = "oss-pulse-ci"
  display_name = "oss-pulse CI (GitHub Actions)"
  description  = "Runs dbt build --target ci from GitHub Actions via Workload Identity Federation."

  depends_on = [google_project_service.apis]
}

# dbt sources read raw.events.
resource "google_bigquery_dataset_iam_member" "ci_raw_viewer" {
  count = local.ci_enabled ? 1 : 0

  dataset_id = google_bigquery_dataset.this["raw"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.ci[0].member
}

# dbt builds seeds, views and tables in the CI dataset only.
resource "google_bigquery_dataset_iam_member" "ci_dataset_editor" {
  count = local.ci_enabled ? 1 : 0

  dataset_id = google_bigquery_dataset.ci[0].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.ci[0].member
}

# Running query/load jobs can only be granted at project level.
resource "google_project_iam_member" "ci_job_user" {
  count = local.ci_enabled ? 1 : 0

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.ci[0].member
}

# Deleted pools stay reserved for 30 days: after infra-down, change wif_pool_id to re-create sooner.
resource "google_iam_workload_identity_pool" "github" {
  count = local.ci_enabled ? 1 : 0

  workload_identity_pool_id = var.wif_pool_id
  display_name              = "GitHub Actions"
  description               = "OIDC tokens from GitHub Actions for ${var.github_repository}."

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  count = local.ci_enabled ? 1 : 0

  workload_identity_pool_id          = google_iam_workload_identity_pool.github[0].workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  # Reject tokens from any other repository before they reach IAM.
  attribute_condition = "assertion.repository == \"${var.github_repository}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Only workflow runs of this repository may act as the CI service account.
resource "google_service_account_iam_member" "ci_wif_user" {
  count = local.ci_enabled ? 1 : 0

  service_account_id = google_service_account.ci[0].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github[0].name}/attribute.repository/${var.github_repository}"
}

# --- Slim CI: compare pull requests with prod (dbt state:modified+ --defer) ----------------------

# Read the prod manifest that Airflow publishes, and nothing else in the lake bucket.
resource "google_storage_bucket_iam_member" "ci_prod_manifest_reader" {
  count = local.ci_enabled ? 1 : 0

  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.ci[0].member

  condition {
    title       = "dbt-prod-state-only"
    description = "Only objects under dbt/prod/ (the prod manifest for Slim CI)."
    expression  = "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.lake.name}/objects/dbt/prod/\")"
  }
}

# --defer reads unchanged parents from prod instead of rebuilding them: read-only, never write.
resource "google_bigquery_dataset_iam_member" "ci_prod_viewer" {
  for_each = local.ci_enabled ? toset(["staging", "marts"]) : toset([])

  dataset_id = google_bigquery_dataset.this[each.key].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.ci[0].member
}
