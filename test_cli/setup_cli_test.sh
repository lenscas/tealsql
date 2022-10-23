#!/bin/bash

VERSION_INFO=$(lua -v)
FEATURE="lua51"

if [[ "$VERSION_INFO" == *"5.4"* ]]; then
    FEATURE="lua54"
elif [[ "$VERSION_INFO" == *"5.3"* ]]; then
    FEATURE="lua53"
elif [[ "$VERSION_INFO" == *"5.2"* ]]; then
    FEATURE="lua52"
else
    FEATURE="lua51"
fi

echo "Using lua version: " $FEATURE

cd pgteal_cli
cargo build --features $FEATURE
cd ../pgteal
cargo build --lib --features $FEATURE,vendored
cargo run --bin main --features $FEATURE,vendored > ../tealsql.json
cd ../
tealr_doc_gen run
cd ./test_cli
cp ../target/debug/libpgteal.so ./cli_test/libpgteal.so
cp ../target/debug/libpgteal.d ./cli_test/libpgteal.d
cp ../pages/tealsql/definitions/tealsql.d.tl ./cli_test/libpgteal.d.tl
../target/debug/pgteal_cli --config config.toml