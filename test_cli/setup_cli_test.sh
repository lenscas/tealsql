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

echo $FEATURE

cd pgteal_cli
cargo build --features $FEATURE
cd ../pgteal
cargo build --lib --features $FEATURE
cargo build --bin main --features $FEATURE
cd ../test_cli
cp ../target/debug/libpgteal.so ./cli_test/libpgteal.so
cp ../target/debug/libpgteal.d ./cli_test/libpgteal.d
../target/debug/main > ./cli_test/libpgteal.d.tl
../target/debug/pgteal_cli --connection postgres://tealsql:tealsql@localhost/tealsql --sqlPattern cli_test/**/*.sql --tealPattern {dir}/{name}_{ext}.tl