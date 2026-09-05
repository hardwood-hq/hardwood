#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

FROM quay.io/fedora/fedora-minimal:43

RUN microdnf install -y --nodocs \
      curl \
      git \
      gh \
      jq \
      zip \
      unzip \
      tar \
      diffutils \
      ShellCheck \
      binutils \
      gcc \
      glibc-devel \
      zlib-devel \
      file \
      poppler-utils \
      vim-common \
      util-linux-script \
      procps-ng \
      protobuf-compiler \
      actionlint \
      fontconfig \
      dejavu-sans-fonts \
      google-noto-sans-vf-fonts \
      librsvg2-tools \
      python3 \
      python3-pip \
    && microdnf clean all

# Install SDKMAN and Java 25 (Temurin)
ENV SDKMAN_DIR="/root/.sdkman"
RUN curl -s "https://get.sdkman.io" | bash \
    && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sdk install java 25-tem"
ENV PATH="$SDKMAN_DIR/candidates/java/current/bin:$PATH"
ENV JAVA_HOME="$SDKMAN_DIR/candidates/java/current"

# Install the Hardwood CLI from a tagged GitHub release. The tarball ships the
# native binary alongside a lib/ of codec .so files it resolves relative to its
# real path, so keep them together under /opt and symlink only the launcher.
ARG HARDWOOD_CLI_VERSION=1.0.0.Final
RUN arch="$(uname -m)" \
    && url="https://github.com/hardwood-hq/hardwood/releases/download/v${HARDWOOD_CLI_VERSION}/hardwood-cli-${HARDWOOD_CLI_VERSION}-linux-${arch}.tar.gz" \
    && curl -fsSL "$url" | tar -xz -C /opt \
    && ln -s "/opt/hardwood-cli-${HARDWOOD_CLI_VERSION}-linux-${arch}/bin/hardwood" /usr/local/bin/hardwood \
    && hardwood --version

# Install Claude Code (native installer)
RUN curl -fsSL https://claude.ai/install.sh | bash

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /workspace

# Set up Python venv for test data generation (simple-datagen.py)
COPY requirements.txt .
RUN python3 -m venv .docker-venv \
    && .docker-venv/bin/pip install --no-cache-dir -r requirements.txt

CMD ["claude"]
