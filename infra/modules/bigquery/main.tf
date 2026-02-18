resource "google_bigquery_dataset" "this" {
  project                     = var.project_id
  dataset_id                  = var.dataset_id
  friendly_name               = var.friendly_name
  description                 = "${var.dataset_id} Dataset"
  location                    = var.location
  default_table_expiration_ms = var.expiration_ms

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.this.id
  }
}

data "google_project" "project" {}

resource "google_kms_crypto_key_iam_member" "bigquery_default_kms_key_iam" {
  crypto_key_id = google_kms_crypto_key.this.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}


resource "google_bigquery_dataset" "default" {
  dataset_id                  = "foo"
  friendly_name               = "test"
  description                 = "This is a test description"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "this" {
  for_each            = var.tables
  dataset_id          = google_bigquery_dataset.this.dataset_id
  table_id            = each.key
  schema              = each.value.schema
  deletion_protection = var.env == "prod" ? true : false
  dynamic "time_partitioning" {
    for_each = each.value.partition_field != null ? [1] : []
    content {
      type  = "DAY"
      field = each.value.partition_field
    }
  }
}