output "bucket_url" {
  value = google_storage_bucket.lake.url
}

output "bq_dataset_raw" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_analytics" {
  value = google_bigquery_dataset.analytics.dataset_id
}
