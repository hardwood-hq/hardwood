#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

FROM quay.io/fedora/fedora-minimal:44

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
      libdeflate \
      file \
      poppler-utils \
      vim-common \
      util-linux-script \
      procps-ng \
      time \
      hyperfine \
      fontconfig \
      dejavu-sans-fonts \
      google-noto-sans-vf-fonts \
      librsvg2-tools \
      python3 \
      python3-pip \
    && microdnf clean all

# Distrust the two roots that chain to eMudhra's compromised intermediates
# (https://emudhra.we-have-a-problem.com/). Fedora drops them in
# ca-certificates 2026.2.92; this blocklist covers images built before that
# version reaches the stable repository.
RUN d=/etc/pki/ca-trust/source/blocklist \
    && trust extract --overwrite --format=pem-bundle \
         --filter='pkcs11:object=Comodo%20AAA%20Services%20root;type=cert' \
         "$d/Comodo_AAA_Services_root.pem" \
    && trust extract --overwrite --format=pem-bundle \
         --filter='pkcs11:object=emSign%20ECC%20Root%20CA%20-%20G3;type=cert' \
         "$d/emSign_ECC_Root_CA_-_G3.pem" \
    && update-ca-trust

# `gh stack`, the official extension, registers existing pull requests as a GitHub stack
# (`gh stack link <pr>...`), which `gh` itself cannot do. It installs under /root/.local/share
# rather than the mounted config volume, so it has to be baked in. Pinned because the extension is
# young enough that its command surface is still moving.
ARG GH_STACK_VERSION=v0.1.1
RUN gh extension install github/gh-stack --pin "$GH_STACK_VERSION"

# actionlint checks the GitHub workflows, running ShellCheck over their `run:` scripts. Fedora
# does not package it, so the release archive is pinned by version and SHA-256.
ARG ACTIONLINT_VERSION=1.7.12
ARG ACTIONLINT_SHA256_X86_64=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
ARG ACTIONLINT_SHA256_AARCH64=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
RUN case "$(uname -m)" in \
      x86_64) arch=amd64 sha256="$ACTIONLINT_SHA256_X86_64" ;; \
      aarch64) arch=arm64 sha256="$ACTIONLINT_SHA256_AARCH64" ;; \
      *) echo "No actionlint build for $(uname -m)" >&2; exit 1 ;; \
    esac \
    && archive="/tmp/actionlint.tar.gz" \
    && curl -fsSL -o "$archive" "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_${arch}.tar.gz" \
    && echo "$sha256  $archive" | sha256sum -c - \
    && tar -xzf "$archive" -C /usr/local/bin actionlint \
    && rm "$archive" \
    && actionlint -version

# Install SDKMAN and Java 25 (Temurin). The GA build (25-tem) can leave a virtual thread parked
# forever under timed blocking-queue waits (JDK-8369227, fixed in 25.0.3), which hangs readers (#1230).
ENV SDKMAN_DIR="/root/.sdkman"
RUN curl -s "https://get.sdkman.io" | bash \
    && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sdk install java 25.0.4-tem"
# Temurin ships its own cacerts; point it at the system trust store so the JDK
# follows the same distrust decisions as curl, git and Python.
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts \
      "$(readlink -f "$SDKMAN_DIR/candidates/java/current")/lib/security/cacerts"
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

# Remotes are often added by their SSH URL, but the container has no SSH key; over HTTPS the
# github-proxy service of docker-compose.yaml authenticates them.
RUN git config --system url."https://github.com/".insteadOf git@github.com: \
    && git config --system --add url."https://github.com/".insteadOf ssh://git@github.com/

# Install Claude Code (native installer)
RUN curl -fsSL https://claude.ai/install.sh | bash

COPY etc/dev-container/claude-entrypoint.sh /usr/local/bin/claude-entrypoint

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /workspace

# Set up Python venv for test data generation (simple-datagen.py)
COPY requirements.txt .
RUN python3 -m venv .docker-venv \
    && .docker-venv/bin/pip install --no-cache-dir -r requirements.txt

CMD ["claude"]
