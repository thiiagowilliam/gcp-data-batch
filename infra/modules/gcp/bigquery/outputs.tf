output "dataset_ids" {
  value = { for k, v in google_bigquery_dataset.this : k => v.dataset_id }
}