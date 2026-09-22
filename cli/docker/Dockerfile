#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Minimal Fedora-based container image for the hardwood native CLI.
# Build context must be the cli/ module directory so that target/ is reachable.
# Run: docker build -t ghcr.io/hardwood-hq/hardwood:local -f docker/Dockerfile .

FROM quay.io/fedora/fedora-minimal:43

LABEL org.opencontainers.image.title="hardwood CLI"
LABEL org.opencontainers.image.description="Hardwood native CLI (minimal Fedora base)"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://github.com/hardwood-hq/hardwood"

ENV PATH=/usr/local/bin:$PATH

# Copy pre-built native executable from the Maven target directory.
COPY target/hardwood-cli /usr/local/bin/hardwood

# Install the self-contained picocli completion script as a profile.d snippet, so
# every interactive login shell registers `hardwood` tab completion automatically.
COPY target/hardwood_completion /etc/profile.d/hardwood-completion.sh

# Entrypoint wrapper: starts an interactive shell when invoked with no arguments,
# or forwards to hardwood otherwise. See docker/entrypoint.sh.
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Ensure all binaries are executable and remove any setuid bits.
RUN chmod 0755 /usr/local/bin/entrypoint.sh && \
    chmod 0755 /usr/local/bin/hardwood && chmod -s /usr/local/bin/hardwood
RUN chmod 0644 /etc/profile.d/hardwood-completion.sh

# Extract the codec native libraries embedded in the binary once, at build time, and load
# them from the image via HARDWOOD_LIB_PATH. The container then needs no writable, executable
# directory at run time, so it also works with --read-only or a noexec /tmp.
RUN hardwood --version >/dev/null && \
    mkdir -p /usr/local/lib/hardwood && \
    for lib in libzstd-jni libsnappyjava liblz4-java libbrotli; do \
      cp /tmp/hardwood-root/hardwood-*-"$lib".so /usr/local/lib/hardwood/"$lib".so || exit 1; \
    done && \
    chmod 0755 /usr/local/lib/hardwood && chmod 0644 /usr/local/lib/hardwood/*.so && \
    rm -rf /tmp/hardwood-root
ENV HARDWOOD_LIB_PATH=/usr/local/lib/hardwood

# Give UID 1000 (group 0) a passwd entry and a writable home, so interactive
# shells resolve a name and HOME instead of "I have no name!" / failing `id`.
RUN echo 'hardwood:x:1000:0:hardwood:/home/hardwood:/bin/bash' >> /etc/passwd && \
    mkdir -p /home/hardwood && chown 1000:0 /home/hardwood
ENV HOME=/home/hardwood

# Run as an unprivileged user.
USER 1000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

