#!/bin/bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Start an interactive login shell when invoked with no arguments (or an explicit
# bash); a login shell sources /etc/profile.d, which registers hardwood completion.
# Otherwise forward all arguments to hardwood. To run a different binary, override
# the entrypoint with `docker run --entrypoint ...`.
if [ $# -eq 0 ] || [ "$1" = "/bin/bash" ] || [ "$1" = "bash" ]; then
  exec /bin/bash -l "${@:2}"
else
  exec /usr/local/bin/hardwood "$@"
fi
