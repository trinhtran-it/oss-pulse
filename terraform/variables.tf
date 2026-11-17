variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Region for the bucket and BigQuery datasets (keep them in the same location)."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment. `dev` enables force_destroy on the bucket and datasets."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be \"dev\" or \"prod\"."
  }
}

variable "bucket_name" {
  description = "Globally unique name of the data-lake bucket."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be 3-63 chars: lowercase letters, digits, '-', '_' or '.'."
  }
}

variable "raw_retention_days" {
  description = "Delete objects under raw/ after this many days."
  type        = number
  default     = 90

  validation {
    condition     = var.raw_retention_days > 0
    error_message = "raw_retention_days must be positive."
  }
}

variable "bq_dataset_raw" {
  description = "Dataset for raw events loaded from GCS."
  type        = string
  default     = "oss_pulse_raw"
}

variable "bq_dataset_staging" {
  description = "Dataset for dbt staging models and seeds."
  type        = string
  default     = "oss_pulse_staging"
}

variable "bq_dataset_marts" {
  description = "Dataset for dbt marts."
  type        = string
  default     = "oss_pulse_marts"
}

variable "dataproc_subnet_cidr" {
  description = "IP range of the subnet used by Dataproc Serverless batches."
  type        = string
  default     = "10.10.0.0/24"

  validation {
    condition     = can(cidrhost(var.dataproc_subnet_cidr, 0))
    error_message = "dataproc_subnet_cidr must be a valid CIDR block, e.g. 10.10.0.0/24."
  }
}
