terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "brand_health_bucket_name" {
  description = "GCS bucket name for raw data storage"
  type        = string
}

# Provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "aiplatform.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com"
  ])

  project = var.project_id
  service = each.value

  disable_dependent_services = false
  disable_on_destroy        = false
}

# GCS Bucket for raw data storage
resource "google_storage_bucket" "brand_health_raw_data" {
  name     = var.brand_health_bucket_name
  location = "US"
  project  = var.project_id

  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  depends_on = [google_project_service.required_apis]
}

# BigQuery datasets
resource "google_bigquery_dataset" "fivetran_raw" {
  dataset_id  = "fivetran_raw"
  project     = var.project_id
  location    = "US"
  description = "Raw data ingested by Fivetran"

  access {
    role          = "OWNER"
    user_by_email = data.google_client_openid_userinfo.me.email
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_bigquery_dataset" "brand_health_dev" {
  dataset_id  = "brand_health_dev"
  project     = var.project_id
  location    = "US"
  description = "Development environment for brand health data"

  access {
    role          = "OWNER"
    user_by_email = data.google_client_openid_userinfo.me.email
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_bigquery_dataset" "brand_health_prod" {
  count       = var.environment == "prod" ? 1 : 0
  dataset_id  = "brand_health_prod"
  project     = var.project_id
  location    = "US"
  description = "Production environment for brand health data"

  access {
    role          = "OWNER"
    user_by_email = data.google_client_openid_userinfo.me.email
  }

  depends_on = [google_project_service.required_apis]
}

# Get current user info
data "google_client_openid_userinfo" "me" {}

# Service account for Cloud Functions
resource "google_service_account" "cloud_functions_sa" {
  account_id   = "brand-health-functions"
  display_name = "Brand Health Cloud Functions Service Account"
  project      = var.project_id
}

# IAM roles for the service account
resource "google_project_iam_member" "cloud_functions_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_project_iam_member" "cloud_functions_secrets" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_project_iam_member" "cloud_functions_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

# Pub/Sub topics for triggering functions
resource "google_pubsub_topic" "twitter_trigger" {
  name    = "twitter-data-fetch"
  project = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_pubsub_topic" "reddit_trigger" {
  name    = "reddit-data-fetch"
  project = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_pubsub_topic" "trends_trigger" {
  name    = "trends-data-fetch"
  project = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_pubsub_topic" "cfpb_trigger" {
  name    = "cfpb-data-fetch"
  project = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Cloud Scheduler jobs
resource "google_cloud_scheduler_job" "twitter_daily" {
  name             = "twitter-daily-fetch"
  project          = var.project_id
  region           = var.region
  description      = "Daily Twitter data fetch"
  schedule         = "0 2 * * *"  # 2 AM daily
  time_zone        = "UTC"
  attempt_deadline = "300s"

  pubsub_target {
    topic_name = google_pubsub_topic.twitter_trigger.id
    data       = base64encode(jsonencode({
      source = "scheduler"
    }))
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_cloud_scheduler_job" "reddit_daily" {
  name             = "reddit-daily-fetch"
  project          = var.project_id
  region           = var.region
  description      = "Daily Reddit data fetch"
  schedule         = "0 3 * * *"  # 3 AM daily
  time_zone        = "UTC"
  attempt_deadline = "300s"

  pubsub_target {
    topic_name = google_pubsub_topic.reddit_trigger.id
    data       = base64encode(jsonencode({
      source = "scheduler"
    }))
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_cloud_scheduler_job" "trends_daily" {
  name             = "trends-daily-fetch"
  project          = var.project_id
  region           = var.region
  description      = "Daily Google Trends data fetch"
  schedule         = "0 4 * * *"  # 4 AM daily
  time_zone        = "UTC"
  attempt_deadline = "300s"

  pubsub_target {
    topic_name = google_pubsub_topic.trends_trigger.id
    data       = base64encode(jsonencode({
      source = "scheduler"
    }))
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_cloud_scheduler_job" "cfpb_daily" {
  name             = "cfpb-daily-fetch"
  project          = var.project_id
  region           = var.region
  description      = "Daily CFPB data fetch"
  schedule         = "0 5 * * *"  # 5 AM daily
  time_zone        = "UTC"
  attempt_deadline = "600s"

  pubsub_target {
    topic_name = google_pubsub_topic.cfpb_trigger.id
    data       = base64encode(jsonencode({
      source = "scheduler"
    }))
  }

  depends_on = [google_project_service.required_apis]
}

# Secret Manager secrets (placeholders - values need to be set manually)
resource "google_secret_manager_secret" "twitter_bearer_token" {
  secret_id = "twitter-bearer-token"
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret" "reddit_credentials" {
  secret_id = "reddit-credentials"
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Outputs
output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "bucket_name" {
  description = "GCS bucket name for raw data"
  value       = google_storage_bucket.brand_health_raw_data.name
}

output "service_account_email" {
  description = "Service account email for Cloud Functions"
  value       = google_service_account.cloud_functions_sa.email
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    fivetran_raw      = google_bigquery_dataset.fivetran_raw.dataset_id
    brand_health_dev  = google_bigquery_dataset.brand_health_dev.dataset_id
    brand_health_prod = var.environment == "prod" ? google_bigquery_dataset.brand_health_prod[0].dataset_id : null
  }
}

output "pubsub_topics" {
  description = "Pub/Sub topic names"
  value = {
    twitter = google_pubsub_topic.twitter_trigger.name
    reddit  = google_pubsub_topic.reddit_trigger.name
    trends  = google_pubsub_topic.trends_trigger.name
    cfpb    = google_pubsub_topic.cfpb_trigger.name
  }
}
