cd pgteal
cargo run --features=lua54 --bin main > ../tealsql.json
cd ../
tealr_doc_gen run
mv pages/tealsql/definitions/tealsql.d.tl tests/libpgteal.d.tl
cd pgteal
cargo build --lib --release --features=lua54
cd ..
cp ./target/release/libpgteal.d ./tests/libpgteal.d 
cp ./target/release/libpgteal.so ./tests/libpgteal.so
cd ./tests
tl check test.tl
tl run test.tl