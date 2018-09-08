#!/usr/bin/env bash

VER=$(grep version gradle.properties | awk -F = '{print $2}' | tr -d "\\r")
IMG=$(grep dockerImageName gradle.properties | awk -F = '{print $2}' | tr -d "\\r")
BUILD_DATE=$(date -u +”%Y-%m-%dT%H:%M:%SZ”)
VCS_REF=$(git describe --tags --always --first-parent)

docker build --build-arg VCS_REF="${VCS_REF}" \
  --build-arg BUILD_DATE="${BUILD_DATE}" \
  --build-arg VERSION="${VER}" \
  -f resources/docker/tensorflow/Dockerfile \
  -t "${IMG}:tf" \
  .
