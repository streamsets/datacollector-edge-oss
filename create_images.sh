#!/usr/bin/env bash

set -e -x

VER=$(grep version gradle.properties | awk -F = '{print $2}' | tr -d "\\r")
IMG=$(grep dockerImageName gradle.properties | awk -F = '{print $2}' | tr -d "\\r")
BUILD_DATE=$(date -u +”%Y-%m-%dT%H:%M:%SZ”)
VCS_REF=$(git describe --tags --always --first-parent)

if [ -z "${DOCKER_BUILD_SKIP}" ]; then
  docker build --build-arg base=arm32v6/alpine --build-arg platform=LinuxArm \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    --build-arg VCS_REF="${VCS_REF}" \
    --build-arg VERSION="${VER}" \
    -t "${IMG}:arm32v6-${VER}" .
  docker build --build-arg base=arm64v8/alpine --build-arg platform=LinuxArm64 \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    --build-arg VCS_REF="${VCS_REF}" \
    --build-arg VERSION="${VER}" \
    -t "${IMG}:arm64v8-${VER}" .
  docker build --build-arg base=alpine --build-arg platform=LinuxAmd64 \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    --build-arg VCS_REF="${VCS_REF}" \
    --build-arg VERSION="${VER}" \
    -t "${IMG}:amd64-${VER}" .
fi

docker push "${IMG}:arm32v6-${VER}"
docker push "${IMG}:arm64v8-${VER}"
docker push "${IMG}:amd64-${VER}"

docker manifest create --amend "${IMG}:${VER}" "${IMG}:amd64-${VER}" "${IMG}:arm32v6-${VER}" "${IMG}:arm64v8-${VER}"
docker manifest annotate "${IMG}:${VER}" "${IMG}:arm32v6-${VER}" --os linux --arch arm
docker manifest annotate "${IMG}:${VER}" "${IMG}:arm64v8-${VER}" --os linux --arch arm64 --variant armv8

docker manifest push --purge "${IMG}:${VER}"