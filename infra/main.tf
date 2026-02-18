module "bigquery_dataset_churn" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  dataset_id        = var.dataset_id
  friendly_name     = var.friendly_name
  location          = var.location
  expiration_ms     = var.expiration_ms
  kms_key           = var.kms_key
  kms_key_ring_name = var.kms_key_ring_name
  env = var.env
  tables = var.tables
}