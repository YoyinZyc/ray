FROM alpine:latest

# Copy the statically built binary from the build context
COPY gcs_sidecar /usr/local/bin/gcs_sidecar

# Ensure it is executable
RUN chmod +x /usr/local/bin/gcs_sidecar

# Run the binary
CMD ["/usr/local/bin/gcs_sidecar"]
