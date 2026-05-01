# SPDX-License-Identifier: Apache-2.0
#
# Copyright The original authors
#
# Licensed under the Apache Software License version 2.0, available at
# http://www.apache.org/licenses/LICENSE-2.0

Output cli/target/dive-screenshots.cast
Set Width 120
Set Height 40

# 01: Overview
Sleep 700ms
Screenshot ../../docs/content/img/screenshots/01-landing-overview.svg

# 02: Schema
Enter
Type "e"
Down
Down
Down
Left
Down
Left
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/02-schema-tree.svg

# 03: Row groups
Escape
Down
Enter
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/03-1-rg.svg
Enter
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/03-2-rg-detail.svg
Enter
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/03-3-rg-column-chunks.svg
Down
Down
Down
Down
Down
Down
Down
Enter
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/03-4-rg-column-chunk-detail.svg

# 04: Pages (with page-header modal)
Enter
Down
Enter
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/04-pages-header-modal.svg
Escape
Escape

# 05: Dictionary with inline search
Down
Enter
Type "/"
Type "r"
Sleep 350ms
Screenshot ../../docs/content/img/screenshots/05-dict-search.svg

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
Screenshot ../../docs/content/img/screenshots/06-data-scrolled-right.svg

# Exit app
Type "q"
