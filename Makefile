all: csv_converter

csv_converter:
	g++ -lpthread -o csv_converter src/main.c src/filestream.c src/csv_stream.c src/process.c src/util.c

clean:
	rm csv_converter
