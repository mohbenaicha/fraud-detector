# Pub/Sub admin
gcloud projects add-iam-policy-binding fraud-app-469623 \
  --member="user:mohamedbenaicha1992@gmail.com" \
  --role="roles/pubsub.admin"

# BigQuery admin
gcloud projects add-iam-policy-binding fraud-app-469623 \
  --member="user:mohamedbenaicha1992@gmail.com" \
  --role="roles/bigquery.admin"

# Vertex AI admin (includes Feature Store)
gcloud projects add-iam-policy-binding fraud-app-469623 \
  --member="user:mohamedbenaicha1992@gmail.com" \
  --role="roles/aiplatform.admin"

# Cloud Functions admin
gcloud projects add-iam-policy-binding fraud-app-469623 \
  --member="user:mohamedbenaicha1992@gmail.com" \
  --role="roles/cloudfunctions.admin"

  # Cloud Storage admin
  gcloud projects add-iam-policy-binding fraud-app-469623 \
    --member="user:mohamedbenaicha1992@gmail.com" \
    --role="roles/storage.admin"