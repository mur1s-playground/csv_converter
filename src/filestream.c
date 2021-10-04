#include "filestream.h"

int filestream_open(struct filestream *fs, string filename, int buffer_size, const char *mode) {
	fs->f_ptr = fopen(filename.c_str(), mode);
	if (fs->f_ptr == NULL) {
		printf("Error opening filestream\n");
		return 0;
	}
	fs->buffer_size = buffer_size;
	fs->buffer = (char *) malloc(buffer_size);
	if (fs->buffer == NULL) {
		printf("Error allocating filestream buffer\n");
		return 0;
	}
	return 1;
}

int filestream_buffer_next(struct filestream *fs) {
	size_t fr = fread(fs->buffer, fs->buffer_size, 1, fs->f_ptr);
	if (fr == 0) {
		int ft = ftell(fs->f_ptr);
		int ft_o = ft % fs->buffer_size;
		fseek(fs->f_ptr, -ft_o, SEEK_CUR);
		fr = fread(fs->buffer, ft_o, 1, fs->f_ptr);
		fs->buffer[ft_o] = '\0';
		fr = 1;
	}
	return (int) fr;
}

int filestream_write_buffer(struct filestream *fs, size_t size) {
	size_t fr = fwrite(fs->buffer, size, 1, fs->f_ptr);
	return (int) fr;
}

void filestream_close(struct filestream *fs) {
	fclose(fs->f_ptr);
}
