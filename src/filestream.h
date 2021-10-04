#ifndef FILESTREAM_H
#define FILESTREAM_H

#include <string>
#include <cstdio>
#include <stdlib.h>

using namespace std;

struct filestream {
	FILE *f_ptr;

	char *buffer;
	int buffer_size;
};

int filestream_open(struct filestream *fs, string filename, int buffer_size, const char *mode);
int filestream_buffer_next(struct filestream *fs);
int filestream_write_buffer(struct filestream *fs, size_t size);
void filestream_close(struct filestream *fs);

#endif
