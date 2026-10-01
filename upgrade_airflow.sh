# # Export values for Airflow docker image
# export IMAGE_NAME=my-dags
# export IMAGE_TAG=0.0.1
# export NAMESPACE=airflow
# export RELEASE_NAME=airflow


# # Build the image and load into Kind
# docker build --pull --tag $IMAGE_NAME:$IMAGE_TAG -f cicd/Dockerfile .
# kind load docker-image $IMAGE_NAME:$IMAGE_TAG

# # Upgrade Airflow using Helm
# helm upgrade "$RELEASE_NAME" apache-airflow/airflow \
#   --namespace "$NAMESPACE" \
#   --create-namespace \
#   -f chart/values-override.yaml \
#   --set-string "images.airflow.repository=$IMAGE_NAME" \
#   --set-string "images.airflow.tag=$IMAGE_TAG" \
#   --server-side=false \
#   --timeout 20m \
#   --debug

# kubectl port-forward svc/$RELEASE_NAME-api-server 8080:8080 --namespace $NAMESPACE

#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=my-dags
IMAGE_TAG=$(date +%Y%m%d%H%M%S)
NAMESPACE=airflow
RELEASE_NAME=airflow

echo "Building ${IMAGE_NAME}:${IMAGE_TAG}"

docker build --pull \
  --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
  -f cicd/Dockerfile .

echo "Loading image into kind"

kind load docker-image \
  "${IMAGE_NAME}:${IMAGE_TAG}" \
  --name kind

echo "Upgrading Airflow"

helm upgrade "$RELEASE_NAME" apache-airflow/airflow \
  --namespace "$NAMESPACE" \
  -f chart/values-override.yaml \
  --set-string "images.airflow.repository=${IMAGE_NAME}" \
  --set-string "images.airflow.tag=${IMAGE_TAG}" \
  --server-side=false \
  --timeout 20m

echo "Upgrade submitted with ${IMAGE_NAME}:${IMAGE_TAG}"

kubectl get pods -n "$NAMESPACE"