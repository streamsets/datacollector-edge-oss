# Additional Dockerfiles

This directory contains Dockerfiles for additional builds for specialized use cases.
The primary Dockerfile is included in the root of the repository.

All images *must* be built from the root of the repository to ensure the build context
contains the necessary files.

## Tensorflow

This image is based on Debian instead of Alpine Linux for compatibility with Tensorflow.
It is only avsailable for 64-bit linux and requires CGO and the Tensorflow shared libraries.
Because of this, it clocks in at a significantly heftier 200MB or so.

GPU support can be enabled by setting the build arg TF_TYPE to "gpu".

### Building

    resources/docker/tensorflow/build.sh
