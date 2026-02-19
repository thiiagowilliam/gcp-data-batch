# 1. COMPOSER
resource "google_service_account" "composer_sa" {
  account_id   = "composer-worker-sa"
  display_name = "Service Account for Cloud Composer Environment"
  project      = var.project_id
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# 