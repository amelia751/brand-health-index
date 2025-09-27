# Cloud Run services for data fetchers (Gen2 functions)

# Twitter Fetcher Cloud Run Service
resource "google_cloud_run_v2_service" "twitter_fetcher" {
  name     = "twitter-fetcher"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloud_functions_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/twitter-fetcher:latest"
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.brand_health_raw_data.name
      }
      
      env {
        name  = "TWITTER_SECRET_NAME"
        value = google_secret_manager_secret.twitter_bearer_token.secret_id
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      
      ports {
        container_port = 8080
      }
    }
    
    timeout = "300s"
    max_instance_request_concurrency = 1
    
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Reddit Fetcher Cloud Run Service
resource "google_cloud_run_v2_service" "reddit_fetcher" {
  name     = "reddit-fetcher"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloud_functions_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/reddit-fetcher:latest"
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.brand_health_raw_data.name
      }
      
      env {
        name  = "REDDIT_SECRET_NAME"
        value = google_secret_manager_secret.reddit_credentials.secret_id
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
      
      ports {
        container_port = 8080
      }
    }
    
    timeout = "600s"
    max_instance_request_concurrency = 1
    
    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Trends Fetcher Cloud Run Service
resource "google_cloud_run_v2_service" "trends_fetcher" {
  name     = "trends-fetcher"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloud_functions_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/trends-fetcher:latest"
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.brand_health_raw_data.name
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      
      ports {
        container_port = 8080
      }
    }
    
    timeout = "300s"
    max_instance_request_concurrency = 1
    
    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }
  }

  depends_on = [google_project_service.required_apis]
}

# CFPB Fetcher Cloud Run Service
resource "google_cloud_run_v2_service" "cfpb_fetcher" {
  name     = "cfpb-fetcher"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloud_functions_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/cfpb-fetcher:latest"
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.brand_health_raw_data.name
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      
      ports {
        container_port = 8080
      }
    }
    
    timeout = "600s"
    max_instance_request_concurrency = 1
    
    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Eventarc triggers for Pub/Sub to Cloud Run
resource "google_eventarc_trigger" "twitter_trigger" {
  name     = "twitter-pubsub-trigger"
  location = var.region
  project  = var.project_id

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.twitter_fetcher.name
      region  = var.region
    }
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.twitter_trigger.id
    }
  }

  service_account = google_service_account.cloud_functions_sa.email

  depends_on = [google_project_service.required_apis]
}

resource "google_eventarc_trigger" "reddit_trigger" {
  name     = "reddit-pubsub-trigger"
  location = var.region
  project  = var.project_id

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.reddit_fetcher.name
      region  = var.region
    }
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.reddit_trigger.id
    }
  }

  service_account = google_service_account.cloud_functions_sa.email

  depends_on = [google_project_service.required_apis]
}

resource "google_eventarc_trigger" "trends_trigger" {
  name     = "trends-pubsub-trigger"
  location = var.region
  project  = var.project_id

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.trends_fetcher.name
      region  = var.region
    }
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.trends_trigger.id
    }
  }

  service_account = google_service_account.cloud_functions_sa.email

  depends_on = [google_project_service.required_apis]
}

resource "google_eventarc_trigger" "cfpb_trigger" {
  name     = "cfpb-pubsub-trigger"
  location = var.region
  project  = var.project_id

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.cfpb_fetcher.name
      region  = var.region
    }
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.cfpb_trigger.id
    }
  }

  service_account = google_service_account.cloud_functions_sa.email

  depends_on = [google_project_service.required_apis]
}

# IAM permissions for Eventarc
resource "google_project_iam_member" "eventarc_agent" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_cloud_run_service_iam_member" "twitter_invoker" {
  service  = google_cloud_run_v2_service.twitter_fetcher.name
  location = google_cloud_run_v2_service.twitter_fetcher.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_cloud_run_service_iam_member" "reddit_invoker" {
  service  = google_cloud_run_v2_service.reddit_fetcher.name
  location = google_cloud_run_v2_service.reddit_fetcher.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_cloud_run_service_iam_member" "trends_invoker" {
  service  = google_cloud_run_v2_service.trends_fetcher.name
  location = google_cloud_run_v2_service.trends_fetcher.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

resource "google_cloud_run_service_iam_member" "cfpb_invoker" {
  service  = google_cloud_run_v2_service.cfpb_fetcher.name
  location = google_cloud_run_v2_service.cfpb_fetcher.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

# Glassdoor Fetcher Cloud Run Service (RapidAPI)
resource "google_cloud_run_v2_service" "glassdoor_fetcher" {
  name     = "glassdoor-fetcher"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloud_functions_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/glassdoor-fetcher:rapidapi"
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.brand_health_raw_data.name
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "2Gi"
        }
      }
      
      ports {
        container_port = 8080
      }
    }
    
    timeout = "1800s"  # 30 minutes timeout for API calls
    max_instance_request_concurrency = 1
  }
  
  traffic {
    percent = 100
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  }
}

# Glassdoor Fetcher IAM
resource "google_cloud_run_service_iam_member" "glassdoor_invoker" {
  service  = google_cloud_run_v2_service.glassdoor_fetcher.name
  location = google_cloud_run_v2_service.glassdoor_fetcher.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_functions_sa.email}"
}

# Glassdoor Pub/Sub Topic
resource "google_pubsub_topic" "glassdoor_trigger" {
  name = "glassdoor-data-fetch"
}

# Glassdoor Eventarc Trigger
resource "google_eventarc_trigger" "glassdoor_trigger" {
  name     = "glassdoor-pubsub-trigger"
  location = var.region
  project  = var.project_id
  
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }
  
  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.glassdoor_fetcher.name
      region  = var.region
    }
  }
  
  transport {
    pubsub {
      topic = google_pubsub_topic.glassdoor_trigger.id
    }
  }

  service_account = google_service_account.cloud_functions_sa.email

  depends_on = [google_project_service.required_apis]
}

# Glassdoor Fetcher Scheduler Job (Weekly - Glassdoor data doesn't change daily)
resource "google_cloud_scheduler_job" "glassdoor_weekly" {
  name             = "glassdoor-data-fetch-weekly"
  description      = "Weekly Glassdoor company reviews and ratings collection"
  schedule         = "0 6 * * 1"  # 6 AM UTC every Monday
  time_zone        = "UTC"
  attempt_deadline = "3600s"  # 1 hour

  pubsub_target {
    topic_name = google_pubsub_topic.glassdoor_trigger.id
    data       = base64encode(jsonencode({
      source = "scheduler"
      brands = ["jpm", "wf", "bac", "c", "gs", "ms", "td"]
      max_pages = 3
    }))
  }
}

# Outputs for Cloud Run services
output "cloud_run_services" {
  description = "Cloud Run service URLs"
  value = {
    twitter   = google_cloud_run_v2_service.twitter_fetcher.uri
    reddit    = google_cloud_run_v2_service.reddit_fetcher.uri
    trends    = google_cloud_run_v2_service.trends_fetcher.uri
    cfpb      = google_cloud_run_v2_service.cfpb_fetcher.uri
    glassdoor = google_cloud_run_v2_service.glassdoor_fetcher.uri
  }
}
