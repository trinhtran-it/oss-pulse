terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "lake" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_dataset_raw
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = var.bq_dataset_analytics
  location   = var.region
}
