#!/usr/bin/env bash

IMAGE_NAME="${1:-""}"
DEPLOYMENT_NAME="${2:-$CI_PROJECT_NAME}"
CONTAINER_NAME="${3:-app}"
TAG=${CI_COMMIT_SHORT_SHA}

TXT_RED="\e[31m" && TXT_GREEN="\e[0;32m" TXT_CLEAR="\e[0m"

echo "searching TAG '$TAG' for image $CI_REGISTRY_IMAGE/$IMAGE_NAME" >&2


REPO_ID=$(
  curl \
    --silent \
    --show-error \
    --fail \
    --header "PRIVATE-TOKEN: $GITLAB_ACCESS_TOKEN" \
    "https://gitlab.com/api/v4/projects/$CI_PROJECT_ID/registry/repositories" |
    jq -e ".[] | select(.name==\"$IMAGE_NAME\") | .id"
)

TAG=$(
  curl \
    --silent \
    --show-error \
    --header "PRIVATE-TOKEN: $GITLAB_ACCESS_TOKEN" \
    "https://gitlab.com/api/v4/projects/$CI_PROJECT_ID/registry/repositories/$REPO_ID/tags/$TAG" |
    jq -r '.name'
)
# First, check if the IMAGE TAG exists
if [ "$CI_COMMIT_SHORT_SHA" == "$TAG" ]; then
  echo -e "${TXT_GREEN}found TAG $TAG${TXT_CLEAR}" >&2
  if [ -z "$IMAGE_NAME" ]
  then
        echo "$CI_REGISTRY_IMAGE:$TAG"
  else
        echo "$CI_REGISTRY_IMAGE/$IMAGE_NAME:$TAG"
  fi
else
  echo 'Tag not found, getting TAG from current deployment' >&2

  # Point to the internal API server hostname
  APISERVER=https://kubernetes.default.svc
  # Path to ServiceAccount token
  SERVICEACCOUNT=/var/run/secrets/kubernetes.io/serviceaccount
  # Read this Pod's namespace
  # NAMESPACE=$(cat ${SERVICEACCOUNT}/namespace)
  NAMESPACE=default
  # Read the ServiceAccount bearer token
  TOKEN=$(cat ${SERVICEACCOUNT}/token)
  # Reference the internal certificate authority (CA)
  CACERT=${SERVICEACCOUNT}/ca.crt

  echo 'url = ' ${APISERVER}/apis/apps/v1/namespaces/"${NAMESPACE}"/cronjobs/"${DEPLOYMENT_NAME}" >&2

  RESPONSE=$(
    curl \
      --silent \
      --show-error \
      --fail \
      --cacert ${CACERT} \
      --header "Authorization: Bearer ${TOKEN}" \
      ${APISERVER}/apis/apps/v1/namespaces/"${NAMESPACE}"/cronjobs/"${DEPLOYMENT_NAME}"
  )
  IMAGE=$(echo "$RESPONSE" | jq -e -r ".spec.template.spec.containers[] | select(.name==\"$CONTAINER_NAME\") | .image") || exit_code=$?

  if [ $exit_code -ne 0 ]; then
    echo -e "${TXT_RED}Tag not found${TXT_CLEAR}" >&2
    exit $exit_code
  else
    echo -e "${TXT_GREEN}Found IMAGE: $IMAGE${TXT_CLEAR}" >&2
    echo "$IMAGE"
  fi
fi
