#!/usr/bin/env bash

IMAGE_NAME="${1:-""}"
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

echo "Checking if '$TAG' is from commit '$CI_COMMIT_SHORT_SHA'" >&2

# First, check if the IMAGE TAG exists
if [ "$CI_COMMIT_SHORT_SHA" = "$TAG" ]
 then
  echo "${TXT_GREEN}found TAG $TAG${TXT_CLEAR}" >&2
  if [ -z "$IMAGE_NAME" ]
  then
        echo "$CI_REGISTRY_IMAGE:$TAG"
  else
        echo "$CI_REGISTRY_IMAGE/$IMAGE_NAME:$TAG"
  fi
else
  echo "${TXT_RED}Tag not found${TXT_CLEAR}" >&2
  exit 1
fi