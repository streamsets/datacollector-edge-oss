FROM alpine
MAINTAINER Madhu <madhu@streamsets.com>

# Copy the dist tar file to the container's workspace.
ADD dist/streamsets-dataextractor-linux-amd64-0.0.1.tar.gz /

# Run the dataextractor command by default when the container starts.
CMD ["/streamsets-dataextractor/bin/dataextractor"]

# Document that the service listens on port 18633.
EXPOSE 18633
