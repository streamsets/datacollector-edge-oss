# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
FROM golang

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/streamsets/dataextractor

WORKDIR "/go/src/github.com/streamsets/dataextractor"
RUN make

# Run the dataextractor command by default when the container starts.
CMD cd /go/src/github.com/streamsets/dataextractor/dist ; bin/dataextractor

# Document that the service listens on port 18633.
EXPOSE 18633
