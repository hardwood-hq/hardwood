#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

FROM quay.io/fedora/fedora-minimal:41

RUN microdnf install -y --nodocs \
      curl \
      git \
      gh \
      jq \
      zip \
      unzip \
      tar \
    && microdnf clean all

# Install SDKMAN and Java 25 (Temurin)
ENV SDKMAN_DIR="/root/.sdkman"
RUN curl -s "https://get.sdkman.io" | bash \
    && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sdk install java 25-tem"
ENV PATH="$SDKMAN_DIR/candidates/java/current/bin:$PATH"
ENV JAVA_HOME="$SDKMAN_DIR/candidates/java/current"

# Install Claude Code (native installer)
RUN curl -fsSL https://claude.ai/install.sh | bash

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /workspace

CMD ["claude"]
