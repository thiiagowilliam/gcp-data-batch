data "google_project" "this" {
  project_id = var.project_id
}

# Identidade do Storage 
resource "google_project_service_identity" "this" {
  project  = var.project_id
  provider = google-beta
  service  = "storage.googleapis.com"
}

# 1. Permissão para o BIGQUERY
resource "google_kms_crypto_key_iam_member" "bq_kms" {
  crypto_key_id = google_kms_crypto_key.this.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:bq-${data.google_project.this.number}@bigquery-encryption.iam.gserviceaccount.com"
}

# 2. Permissão para o STORAGE 
resource "google_kms_crypto_key_iam_member" "storage_kms" {
  crypto_key_id = google_kms_crypto_key.this.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member = "serviceAccount:service-${data.google_project.this.number}@gs-project-accounts.iam.gserviceaccount.com"
}

resource "google_kms_crypto_key" "this" {
  name     = var.kms_key
  key_ring = google_kms_key_ring.this.id
}

resource "google_kms_key_ring" "this" {
  project  = var.project_id
  name     = var.kms_key_ring
  location = lower(var.location)
}