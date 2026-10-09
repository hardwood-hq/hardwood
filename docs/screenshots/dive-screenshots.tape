#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# The recording output (.cast), terminal size, and fps are supplied by the
# `screenshots` Maven profile via -Dtamboui.record* system properties; this
# tape only scripts the interaction and the Screenshot captures.
#
# Screenshot paths are resolved relative to the .cast file's directory
# (cli/target/), so ../../ points at the repository root.

# 01: Overview
Sleep 700ms
Screenshot ../../docs/content/assets/cli/01-landing-overview.svg

# Row groups -> first row group -> Column chunks -> the eighth chunk's detail
Down
Enter
Sleep 350ms
Enter
Sleep 350ms
Enter
Sleep 350ms
Down
Down
Down
Down
Down
Down
Down
Enter
Sleep 350ms

# 05: Dictionary with inline search (the cursor stops on the Column index and
# Offset index entries on its way down the drill menu)
Down
Down
Down
Enter
Type "/"
Type "r"
Sleep 350ms
Screenshot ../../docs/content/assets/cli/05-dict-search.svg

# 06: Data preview scrolled right
Escape
Escape
Escape
Escape
Escape
Escape
Down
Down
Enter
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Right
Sleep 350ms
Screenshot ../../docs/content/assets/cli/06-data-scrolled-right.svg

# Exit app
Type "q"
