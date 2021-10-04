#ifndef CSV_STREAM_H
#define CSV_STREAM_H

#include "filestream.h"

enum eol_type {
	EOL_TYPE_N,
	EOL_TYPE_RN
};

enum csv_column_type {
	CSV_COLUMN_TYPE_UNBOUNDED_STRING,
	CSV_COLUMN_TYPE_BOUNDED_STRING
};

struct csv_stream {
	char csv_delimiter;
	char csv_string_boundary;
	enum eol_type eol_t;

	int row_buffer_row_count;
	int row_buffer_max_line_length;

	char* row_buffer;

	int max_column_count;
	int* column_end_index;

	int row_position_current_read;
	int column_position_current_read;

	int row_position_parse;

	int filestream_buffer_position;

	enum csv_column_type *output_column_type;
};

int csv_stream_init(struct csv_stream *csvs, char csv_delimiter, char csv_string_boundary, enum eol_type eol_t, int row_buffer_row_count, int row_buffer_max_line_length, int max_column_count, enum csv_column_type *output_column_type = NULL);

int csv_stream_get_row(struct csv_stream *csvs, struct filestream *fs);
void csv_stream_clear_row(struct csv_stream *csvs, int row);
void csv_stream_move_row(struct csv_stream *csvs, int from_row, int to_row);
char *csv_stream_get_col(struct csv_stream *csvs, int row, int col, int* size_out);

void csv_stream_append_col(struct csv_stream *csvs, int row, char *data, int size);
int csv_stream_write_row(struct csv_stream *csvs, struct filestream *fs, int row);

void csv_stream_destroy(struct csv_stream *csvs);

int csv_stream_print_col(struct csv_stream *csvs, int row, int col);
int csv_stream_print_row(struct csv_stream *csvs, int row);
void csv_stream_print_row_w(struct csv_stream *csvs, int row);

#endif
