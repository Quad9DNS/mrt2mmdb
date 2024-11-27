#!/bin/sh

source ./assert.sh

# make mmdb file using input json and run filter.py to trim the mmdb
./mmdbctl import -f json small.json small.mmdb &>/dev/null
../mrt2mmdb/filter.py --mmdb small.mmdb --trim `cat ./ignore.list` --quiet &>/dev/null
assert_raises "true"

# make mmdb file using input json and run filter.py to trim the mmdb
./mmdbctl import -f json medium.json medium.mmdb >/dev/null 2>&1
../mrt2mmdb/filter.py --mmdb medium.mmdb --trim `cat ./ignore.list` --quiet &>/dev/null
assert_raises "true"

verify_small=$(./mmdbctl export -f json small.mmdb | jq -r '.network,.location.latitude,.location.longitude') 
verify_medium=$(./mmdbctl export -f json medium.mmdb | jq -r '.network,.location.latitude,.location.longitude')


alias exec_small="./mmdbctl export -f json small.mmdb.trim | jq -r '.network,.location.latitude,.location.longitude'" 
alias exec_medium="./mmdbctl export -f json medium.mmdb.trim | jq -r '.network,.location.latitude,.location.longitude'" 

# Verify the mmdb.trim vs mmdb file
assert "exec_small" "$verify_small"
assert "exec_medium" "$verify_medium"
assert_end filter.py
