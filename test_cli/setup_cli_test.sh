#!/bin/bash

cd pgteal_cli
cargo build
cd ../pgteal
cargo build --lib
cargo build --bin main
cd ../test_cli
cp ../target/debug/libpgteal.so ./cli_test/libpgteal.so
cp ../target/debug/libpgteal.d ./cli_test/libpgteal.d
../target/debug/main > ./cli_test/libpgteal.d.tl
../target/debug/pgteal_cli --connection postgres://tealsql:tealsql@localhost/tealsql --sqlPattern cli_test/**/*.sql --tealPattern {dir}/{name}_{ext}.tl