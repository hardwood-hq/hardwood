#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# The addon is baked into the image rather than bind-mounted: mitmproxy reloads a script when its
# file changes, and the claude container can write to the repository.
FROM mitmproxy/mitmproxy:12.3.0@sha256:c8a3753de11fd8266afeb103e163f7b7ac6a4a568a707fa2e15b23d20acf4eba

COPY github_token.py /opt/github-proxy/github_token.py

# Only the GitHub hosts are decrypted; other HTTPS traffic tunnels through unread. mitm.it is
# where mitmproxy serves its CA certificate, which the claude container fetches on start.
CMD ["mitmdump", \
     "--set", "allow_hosts=^(api\\.github\\.com|uploads\\.github\\.com|github\\.com|mitm\\.it)(:[0-9]+)?$", \
     "--scripts", "/opt/github-proxy/github_token.py"]
