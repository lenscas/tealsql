cd pgteal
cargo run --features=lua54 --bin main > ../tests/libpgteal.d.tl
cargo build --lib --release --features=lua54
cd ..
cp ./target/release/libpgteal.d ./tests/libpgteal.d 
cp ./target/release/libpgteal.so ./tests/libpgteal.so
cd ./tests
tl check test.tl
tl run test.tl