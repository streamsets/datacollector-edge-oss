FROM alpine
MAINTAINER Madhu <madhu@streamsets.com>

# Copy the dist tar file to the container's workspace.
ADD dist/streamsets-datacollector-edge-linux-amd64-3.0.0.0.tar.gz /

# Run the dataextractor command by default when the container starts.
CMD ["/streamsets-datacollector-edge/bin/edge"]

# Document that the service listens on port 18633.
EXPOSE 18633
