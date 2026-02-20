resource "google_composer_environment" "this" {
  project = var.project_id
  name    = var.composer_name
  region  = var.region

  config {
    node_config {
      service_account = var.composer_sa
    }
    software_config {
      image_version = var.composer_image_version
      env_variables = var.env_variables
      pypi_packages = var.pypi_packages
    }
    workloads_config {
      worker {
        cpu    = 2
        memory_gb = 2
        storage_gb = 5
        min_count = 1
        max_count = 1
      }
    }
  }
}