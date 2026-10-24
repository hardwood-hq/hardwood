#!/bin/sh
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Entrypoint of the claude service in docker-compose.yaml: trusts the CA of the github-proxy
# service, which decrypts the container's traffic to GitHub to add the token, then runs the given
# command.

set -eu

: "${HTTPS_PROXY:?HTTPS_PROXY must point to the github-proxy service}"

curl -fsS --retry 30 --retry-all-errors --retry-delay 1 --proxy "$HTTPS_PROXY" \
  -o /etc/pki/ca-trust/source/anchors/github-proxy.pem http://mitm.it/cert/pem
update-ca-trust

exec "$@"
