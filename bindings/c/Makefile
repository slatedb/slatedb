build:
	cargo build --release
	export LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH
	mkdir -p out
	gcc -I ./include examples/in_memory.c -L ./target/release -lslatedb_c -o out/in_memory.out
	gcc -I ./include examples/s3.c -L ./target/release -lslatedb_c -o out/s3.out

run-memory:
	./out/in_memory.out

run-s3:
	./out/s3.out

run:
	make run-memory
	make run-s3