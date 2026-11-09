#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# The addon is baked into the image rather than bind-mounted: mitmproxy reloads a script when its
# file changes, and the claude container can write to the repository.
FROM mitmproxy/mitmproxy:12.3.1@sha256:2711066c855659b5af8b1dbb84494ed3eff69b42ece439c60b4fe09a2db829d4

COPY github_token.py /opt/github-proxy/github_token.py

# Only the GitHub hosts are decrypted; other HTTPS traffic tunnels through unread. mitm.it is
# where mitmproxy serves its CA certificate, which the claude container fetches on start.
CMD ["mitmdump", \
     "--set", "allow_hosts=^(api\\.github\\.com|uploads\\.github\\.com|github\\.com|mitm\\.it)(:[0-9]+)?$", \
     "--scripts", "/opt/github-proxy/github_token.py"]
