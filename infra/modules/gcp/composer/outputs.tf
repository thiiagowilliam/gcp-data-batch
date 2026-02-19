output "composer_bucket_name" {
  value =  google_composer_environment.this.config[0].dag_gcs_prefix
}