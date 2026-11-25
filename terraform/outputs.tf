output "bucket_name" {
  description = "Data-lake bucket name (use for GCS_BUCKET)."
  value       = google_storage_bucket.lake.name
}

output "bucket_url" {
  description = "Data-lake bucket URL (gs://...)."
  value       = google_storage_bucket.lake.url
}

output "bq_datasets" {
  description = "BigQuery dataset IDs by layer."
  value       = { for layer, ds in google_bigquery_dataset.this : layer => ds.dataset_id }
}

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account."
  value       = google_service_account.pipeline.email
}

output "dataproc_subnet" {
  description = "Subnet name for `gcloud dataproc batches submit --subnet`."
  value       = google_compute_subnetwork.pipeline.name
}

# Set these as GitHub repository variables (Settings -> Secrets and variables -> Actions -> Variables).
output "ci_workload_identity_provider" {
  description = "GitHub variable GCP_WIF_PROVIDER (google-github-actions/auth workload_identity_provider)."
  value       = local.ci_enabled ? google_iam_workload_identity_pool_provider.github[0].name : null
}

output "ci_service_account_email" {
  description = "GitHub variable GCP_CI_SERVICE_ACCOUNT."
  value       = local.ci_enabled ? google_service_account.ci[0].email : null
}
