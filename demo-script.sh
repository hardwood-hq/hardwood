#!/bin/bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Record: asciinema rec demo.cast --overwrite -c "bash demo-script.sh" --cols 120 --rows 30
#
# Play asciinema play demo.cast
#
# Convert to gif via https://dstein64.github.io/gifcast/.

HW=cli/target/hardwood-cli-early-access-macos-aarch64/bin/hardwood
D=core/src/test/resources

first_comment=true
type_comment() {
    if [ "$first_comment" = true ]; then
        first_comment=false
    else
        echo
    fi
    printf "\e[36m"
    for (( i=0; i<${#1}; i++ )); do
        printf "%s" "${1:$i:1}"
        sleep 0.02
    done
    printf "\e[0m\n"
    sleep 0.5
}

# Simulate pasting a path: print it all at once with a tiny pause
paste_path() {
    sleep 0.1
    printf "%s" "$1"
    sleep 0.05
}

type_cmd_with_paste() {
    local prefix="$1"
    local filepath="$2"
    local suffix="$3"
    # Type the command prefix
    for (( i=0; i<${#prefix}; i++ )); do
        printf "%s" "${prefix:$i:1}"
        sleep 0.02
    done
    # Paste the file path instantly
    paste_path "$filepath"
    # Type any suffix
    if [ -n "$suffix" ]; then
        for (( i=0; i<${#suffix}; i++ )); do
            printf "%s" "${suffix:$i:1}"
            sleep 0.02
        done
    fi
    echo
    sleep 0.2
    eval "$(echo "$prefix$filepath$suffix" | sed "s|hardwood |$HW |")"
    sleep 1.5
}

export PS1='\[\e[1;32m\]❯\[\e[0m\] '
alias hardwood=$HW
F1=$D/filter_pushdown_mixed.parquet
F2=$D/nested_struct_test.parquet
F3=$D/page_index_test.parquet

clear
sleep 0.3

type_comment "# File overview"
type_cmd_with_paste "hardwood info -f " "$F1"

type_comment "# Schema with nested structs"
type_cmd_with_paste "hardwood schema -f " "$F2"

type_comment "# First 5 rows of file"
type_cmd_with_paste "hardwood head -f " "$F1" " -n 5"

type_comment "# Page structure: dictionary + data pages"
type_cmd_with_paste "hardwood inspect pages -f " "$F3"

type_comment "# Export as JSON with column selection"
type_cmd_with_paste "hardwood convert -f " "$F1" " --to json --columns id,name,active"

sleep 0.5
