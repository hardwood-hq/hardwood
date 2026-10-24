#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# mitmproxy addon of the github-proxy service in docker-compose.yaml: adds the GitHub token to
# requests the claude container sends to GitHub, so that nothing in the claude container sees the
# token.
#
# Whatever credentials the client sends are replaced, never forwarded: the claude container holds
# only a placeholder token, which `gh` needs in order to run at all. The token goes out only over
# HTTPS on port 443, so that a request for http://github.com/ cannot send it in the clear.

import base64
import os

from mitmproxy import http

TOKEN = os.environ.get("GH_TOKEN", "")
if not TOKEN:
    raise RuntimeError("GH_TOKEN is not set for the github-proxy service")

# The REST and GraphQL API take the token as such; git's smart HTTP protocol on github.com takes
# it as the password of Basic authentication.
BASIC_CREDENTIALS = base64.b64encode(f"x-access-token:{TOKEN}".encode()).decode()
AUTHORIZATION = {
    "api.github.com": f"token {TOKEN}",
    "uploads.github.com": f"token {TOKEN}",
    "github.com": f"Basic {BASIC_CREDENTIALS}",
}


def request(flow: http.HTTPFlow) -> None:
    if flow.request.scheme != "https" or flow.request.port != 443:
        return
    authorization = AUTHORIZATION.get(flow.request.host.lower())
    if authorization is not None:
        flow.request.headers["Authorization"] = authorization

