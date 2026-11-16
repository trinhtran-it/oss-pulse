variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bucket_name" {
  type = string
}

variable "bq_dataset_raw" {
  type    = string
  default = "oss_pulse_raw"
}

variable "bq_dataset_analytics" {
  type    = string
  default = "oss_pulse"
}
