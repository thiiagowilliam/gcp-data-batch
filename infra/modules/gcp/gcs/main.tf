resource "google_storage_bucket" "this" {
  project  = var.project_id
  name     = var.bucket_name
  location = var.location
  uniform_bucket_level_access = var.uniform_bucket_level_access

  versioning {
    enabled = var.bucket_versioning
  }

  encryption {
    default_kms_key_name = var.kms_key
  }
}