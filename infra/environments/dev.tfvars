project_id = "project-abdcb642-e2ac-47c0-b1d"
region = "us-east1"
location = "US"
env="dev"

# GCS
bucket_name = "xlp67-interprise-bucket"
bucket_versioning = true
uniform_bucket_level_access = true

# KMS
kms_key = "gcs-bucket"
kms_key_ring = "gcs-bucket"

# COMPOSER
composer_name="composer-dev"
composer_image_version = "composer-3-airflow-2"

# CLOUDBUILD
github_branch="develop"
github_owner="thiiagowilliam"
github_repo = "gcp-data-batch"
app_installation_id="111001074" 
included_files = "airflow/dags/**"
cloudbuild_trigger_name="trigger-name"
cloudbuild_trigger_path=".github/workflows/cloudbuild.yaml"
oauth_token_secret = "projects/327909419888/secrets/gcp-data-batch-github-oauthtoken-7e8c1d/versions/1"

# BIGQUERY
dataset_id = "churn"
friendly = "Dados de Churn"
expiration_ms = 3600000
