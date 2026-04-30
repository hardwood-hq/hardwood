#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import re

PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def on_page_markdown(markdown, page, config, files):
    extra = config.get("extra") or {}

    def replace(match):
        name = match.group(1)
        value = extra.get(name)
        return str(value) if value is not None else match.group(0)

    return PLACEHOLDER.sub(replace, markdown)
