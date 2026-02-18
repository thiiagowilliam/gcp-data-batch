resource "google_kms_crypto_key" "this" {
  name     = var.kms_key
  key_ring = google_kms_key_ring.this.id
}

resource "google_kms_key_ring" "this" {
  project  = var.project_id
  name     = var.kms_key_ring_name
  location = lower(var.location)
}
