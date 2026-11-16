terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Partial configuration: bucket/prefix come from backend.hcl (git-ignored).
  #   terraform init -backend-config=backend.hcl
  # The state bucket is created manually once (see `make tf-bootstrap`), never by this config.
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  is_dev = var.environment == "dev"

  labels = {
    project     = "oss-pulse"
    environment = var.environment
    managed_by  = "terraform"
  }

  # Terraform owns all datasets; dbt only writes into them (see README "Infrastructure").
  datasets = {
    raw     = { id = var.bq_dataset_raw, description = "Raw GH Archive events loaded from GCS (Spark output)." }
    staging = { id = var.bq_dataset_staging, description = "dbt staging models and seeds." }
    marts   = { id = var.bq_dataset_marts, description = "dbt marts consumed by the dashboard." }
  }
}

# --- APIs ---------------------------------------------------------------------

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}

# --- Data lake ----------------------------------------------------------------

resource "google_storage_bucket" "lake" {
  name                        = var.bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = local.is_dev # dev only: allows `terraform destroy` with objects inside
  labels                      = local.labels

  lifecycle_rule {
    condition {
      age            = var.raw_retention_days
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# --- BigQuery -----------------------------------------------------------------

resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id                 = each.value.id
  description                = each.value.description
  location                   = var.region # same region as the bucket: required for load jobs
  delete_contents_on_destroy = local.is_dev
  labels                     = merge(local.labels, { layer = each.key })

  depends_on = [google_project_service.apis]
}

# --- Pipeline service account (least privilege) -------------------------------

resource "google_service_account" "pipeline" {
  account_id   = "oss-pulse-pipeline"
  display_name = "oss-pulse pipeline"
  description  = "Runs ingestion, Spark, BigQuery loads and dbt for oss-pulse."

  depends_on = [google_project_service.apis]
}

# Read/write objects in the lake bucket only (not project-wide).
resource "google_storage_bucket_iam_member" "pipeline_lake_object_admin" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline.member
}

# Read/write tables in the three project datasets only (not project-wide).
resource "google_bigquery_dataset_iam_member" "pipeline_data_editor" {
  for_each = google_bigquery_dataset.this

  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline.member
}

# Running query/load jobs can only be granted at project level.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.pipeline.member
}

# TODO (if Dataproc Serverless is used for Spark): add roles/dataproc.worker and
# grant the caller roles/iam.serviceAccountUser on this service account.
