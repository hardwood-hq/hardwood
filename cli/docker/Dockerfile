# SPDX-License-Identifier: Apache-2.0
# Minimal Fedora-based container image for the hardwood native CLI.
# This Dockerfile expects a pre-built native Linux `hardwood` executable
# to be present in the build context (CLI CI will place the binary here
# before building the image).

FROM quay.io/fedora/fedora-minimal:41

LABEL org.opencontainers.image.title="hardwood CLI"
LABEL org.opencontainers.image.description="Hardwood native CLI (minimal Fedora base)"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ENV PATH=/usr/local/bin:$PATH

# Copy pre-built native executable into the image. The CI will copy
# the correct architecture-specific binary into this directory before
# invoking `docker build`.
COPY hardwood /usr/local/bin/hardwood

# Copy shell completion script (optional; sourced by users in interactive shells).
COPY hardwood_completion /usr/local/bin/hardwood_completion

# Create an entrypoint wrapper that starts an interactive shell when invoked
# with no arguments, or runs hardwood otherwise.
RUN cat > /usr/local/bin/entrypoint.sh << 'EOF'
#!/bin/bash
if [ $# -eq 0 ] || [ "$1" = "/bin/bash" ] || [ "$1" = "bash" ]; then
  # Interactive mode: source completion via --rcfile.
  exec /bin/bash --rcfile /usr/local/bin/hardwood_completion "${@:2}"
elif [ -x "$1" ]; then
  # Other shell or executable override.
  exec "$@"
else
  # Command mode: run hardwood with arguments.
  exec /usr/local/bin/hardwood "$@"
fi
EOF

# Ensure all binaries are executable and remove any setuid bits.
RUN chmod 0755 /usr/local/bin/entrypoint.sh && \
    chmod 0755 /usr/local/bin/hardwood && chmod -s /usr/local/bin/hardwood
RUN chmod 0644 /usr/local/bin/hardwood_completion

# Run as an unprivileged numeric user (no passwd entry required).
USER 1000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

