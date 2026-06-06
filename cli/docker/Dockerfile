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

# Ensure the binary is executable and remove any setuid bits.
RUN chmod 0755 /usr/local/bin/hardwood && chmod -s /usr/local/bin/hardwood
RUN chmod 0644 /usr/local/bin/hardwood_completion

# Run as an unprivileged numeric user (no passwd entry required).
USER 1000

ENTRYPOINT ["/usr/local/bin/hardwood"]

