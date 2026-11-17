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
