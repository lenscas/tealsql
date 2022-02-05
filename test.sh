cd pgteal
cargo run --bin main > ../tests/libpgteal.d.tl
cargo build --release
cd ..
cp ./target/release/libpgteal.d ./tests/libpgteal.d 
cp ./target/release/libpgteal.so ./tests/libpgteal.so
cd ./tests
tl check test.tl
tl run test.tl