

resource "google_cloudbuild_trigger" "this" {
  location = var.region
  name     = var.cb_trigger_name
  filename = var.cb_trigger_path

  github {
    owner = var.github_owner
    name  = var.github__name
    push {
      branch = var.github_branch
    }
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}