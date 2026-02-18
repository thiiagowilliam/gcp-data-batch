resource "google_project_service_identity" "gcs_sa" {
  provider = google-beta
  service  = "storage.googleapis.com"
}

resource "google_kms_crypto_key_iam_member" "gcs_kms_user" {
  crypto_key_id = var.kms_key_id 
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${google_project_service_identity.gcs_sa.email}"
}

resource "google_storage_bucket" "bucket_seguro" {
  name     = var.bucket_name
  location = var.bucket_location
  versioning {
    enabled = var.bucket_versioning
  }
  encryption {
    default_kms_key_name = var.kms_key_id
  }
  depends_on = [google_kms_crypto_key_iam_member.gcs_kms_user]
}