#include "csv_stream.h"

#include <stdio.h>
#include <cstdlib>
#include <cstring>
#include <math.h>

int csv_stream_init(struct csv_stream *csvs, char csv_delimiter, char csv_string_boundary, enum eol_type eol_t, int row_buffer_row_count, int row_buffer_max_line_length, int max_column_count, enum csv_column_type *output_column_type) {
	csvs->csv_delimiter = csv_delimiter;
	csvs->csv_string_boundary = csv_string_boundary;
	csvs->eol_t = eol_t;

	csvs->row_buffer_row_count = row_buffer_row_count;
	csvs->row_buffer_max_line_length = row_buffer_max_line_length;

	csvs->row_buffer = (char*)malloc(row_buffer_row_count * row_buffer_max_line_length);
	if (csvs->row_buffer == NULL) {
		printf("Error allocating row buffer\n");
		return 0;
	}

	csvs->max_column_count = max_column_count;

	if (csvs->output_column_type == NULL) {
		csvs->column_end_index = (int *)malloc(max_column_count * row_buffer_row_count * sizeof(int));
		if (csvs->column_end_index == NULL) {
			printf("Error allocating column_end_indices\n");
			return 0;
		}
		memset(csvs->column_end_index, 0, max_column_count * row_buffer_row_count * sizeof(int));
	} else {
		csvs->column_end_index = (int *)malloc(2 * row_buffer_row_count * sizeof(int));
		memset(csvs->column_end_index, 0, 2 * row_buffer_row_count * sizeof(int));
	}

	csvs->row_position_current_read = -1;
	csvs->column_position_current_read = -1;

	csvs->row_position_parse = -1;

	csvs->filestream_buffer_position = 0;

	csvs->output_column_type = output_column_type;

	return 1;
}

int csv_stream_get_row(struct csv_stream *csvs, struct filestream *fs) {
	if (csvs->row_position_current_read == -1) {
		filestream_buffer_next(fs);
	}
	int found_end_of_row = -1;
	char last_char = '\0';
	int current_col = 0;
	int current_row = (csvs->row_position_current_read + 1) % csvs->row_buffer_row_count;
	int row_position = 0;
	int eof_pos = -1;
	while (found_end_of_row == -1) {
//		int string_started = 0;
		for (int c = csvs->filestream_buffer_position; c < fs->buffer_size; c++) {
//			printf("%c", fs->buffer[c]);
			if (fs->buffer[c] == csvs->csv_delimiter) {
				csvs->column_end_index[current_row * csvs->max_column_count + current_col] = row_position + c - csvs->filestream_buffer_position;
//				printf("%i\t", csvs->column_end_index[current_row * csvs->max_column_count + current_col]);
				current_col++;
			}
			if ((csvs->eol_t == EOL_TYPE_N && fs->buffer[c] == '\n') || (csvs->eol_t == EOL_TYPE_RN && fs->buffer[c] == '\n' && last_char == '\r')) {
				if (csvs->eol_t == EOL_TYPE_N) {
					csvs->column_end_index[current_row * csvs->max_column_count + current_col] = row_position + c - csvs->filestream_buffer_position - 1;
				} else if (csvs->eol_t == EOL_TYPE_RN) {
					csvs->column_end_index[current_row * csvs->max_column_count + current_col] = row_position + c - csvs->filestream_buffer_position - 2;
				}
//				printf("%i\t", csvs->column_end_index[current_row * csvs->max_column_count + current_col]);
				current_col++;
				csvs->column_end_index[current_row * csvs->max_column_count + current_col] = -1;
//				printf("%i", csvs->column_end_index[current_row * csvs->max_column_count + current_col]);
				found_end_of_row = c;
				break;
			}
			if (fs->buffer[c] == '\0') {
				csvs->column_end_index[current_row * csvs->max_column_count + current_col] = row_position + c - csvs->filestream_buffer_position - 1;
//				printf("%i\t", csvs->column_end_index[current_row * csvs->max_column_count + current_col]);
                                current_col++;
                                csvs->column_end_index[current_row * csvs->max_column_count + current_col] = -1;
//                              printf("%i", csvs->column_end_index[current_row * csvs->max_column_count + current_col]);
				found_end_of_row = -2;
				eof_pos = c;
				break;
			}
			last_char = fs->buffer[c];
		}

		if (found_end_of_row == -1) {
			memcpy(&csvs->row_buffer[current_row * csvs->row_buffer_max_line_length + row_position], &fs->buffer[csvs->filestream_buffer_position], fs->buffer_size - csvs->filestream_buffer_position);
			row_position += fs->buffer_size - csvs->filestream_buffer_position;

			filestream_buffer_next(fs);
			csvs->filestream_buffer_position = 0;
		} else if (found_end_of_row == -2) {
			memcpy(&csvs->row_buffer[current_row * csvs->row_buffer_max_line_length + row_position], &fs->buffer[csvs->filestream_buffer_position], eof_pos - csvs->filestream_buffer_position);
		} else {
			memcpy(&csvs->row_buffer[current_row * csvs->row_buffer_max_line_length + row_position], &fs->buffer[csvs->filestream_buffer_position], found_end_of_row - csvs->filestream_buffer_position);
			row_position += found_end_of_row - csvs->filestream_buffer_position;
			csvs->filestream_buffer_position = found_end_of_row + 1;

			if (csvs->filestream_buffer_position == fs->buffer_size) {
				filestream_buffer_next(fs);
				csvs->filestream_buffer_position = 0;
			}
		}
	}
	if (found_end_of_row >= 0 || found_end_of_row == -2) {
		csvs->row_position_current_read = current_row;
//		csv_stream_print_row(csvs, current_row);
//		printf("\n");
	}
	return found_end_of_row;
}

void csv_stream_clear_row(struct csv_stream *csvs, int row) {
	csvs->column_end_index[row * csvs->max_column_count] = -1;
	csvs->row_position_current_read = row;
}

void csv_stream_move_row(struct csv_stream *csvs, int from_row, int to_row) {
	int from_row_offset = from_row * csvs->row_buffer_max_line_length;
	int to_row_offset = to_row * csvs->row_buffer_max_line_length;
	memcpy(&csvs->row_buffer[to_row_offset], &csvs->row_buffer[from_row_offset], csvs->row_buffer_max_line_length);

	int column_end_indices_from_offset = from_row * csvs->max_column_count;
	int column_end_indices_to_offset = to_row * csvs->max_column_count;
	memcpy(&csvs->column_end_index[column_end_indices_to_offset], &csvs->column_end_index[column_end_indices_from_offset], csvs->max_column_count * sizeof(int));
	csvs->column_end_index[column_end_indices_from_offset] = -1;

	csvs->row_position_current_read = to_row;
}

char *csv_stream_get_col(struct csv_stream *csvs, int row, int col, int* size_out) {
	int row_offset = row * csvs->row_buffer_max_line_length;
	char *col_start = NULL;
	if (col == 0) {
		int size = csvs->column_end_index[row * csvs->max_column_count];
		*size_out = size;
//		printf("size_out: %i\n", csvs->column_end_index[row * csvs->max_column_count]);
		col_start = &csvs->row_buffer[row_offset];
	} else {
		int size = csvs->column_end_index[row * csvs->max_column_count + col] - csvs->column_end_index[row * csvs->max_column_count + col - 1] - 1;
		*size_out = size;
		if (size < 0) {
			printf("WARNING: requested column with negative size\n");
		}
		col_start = &csvs->row_buffer[row_offset + csvs->column_end_index[row * csvs->max_column_count + col - 1] + 1];
	}
	return col_start;
}

void csv_stream_append_col(struct csv_stream *csvs, int row, char *data, int size) {
	int row_offset = row * csvs->row_buffer_max_line_length;

	int position = csvs->column_end_index[2 * row];
	int col = csvs->column_end_index[2 * row + 1];
	if (col > 0) {
		csvs->row_buffer[row_offset + position] = csvs->csv_delimiter;
		position++;
	}
	if (csvs->output_column_type[col] == CSV_COLUMN_TYPE_BOUNDED_STRING) {
		csvs->row_buffer[row_offset + position] = csvs->csv_string_boundary;
		position++;
	}

	memcpy(&csvs->row_buffer[row_offset + position], data, size);
	position += size;

	if (csvs->output_column_type[col] == CSV_COLUMN_TYPE_BOUNDED_STRING) {
                csvs->row_buffer[row_offset + position] = csvs->csv_string_boundary;
                position++;
        }
	csvs->column_end_index[2 * row] = position;
	csvs->column_end_index[2 * row + 1]++;

//	csv_stream_print_row_w(csvs, row);
}

int csv_stream_write_row(struct csv_stream *csvs, struct filestream *fs, int row) {
	int row_offset = row * csvs->row_buffer_max_line_length;

	int row_length = csvs->column_end_index[2 * row];
	if (csvs->eol_t == EOL_TYPE_RN) {
		csvs->row_buffer[row_offset + row_length] = '\r';
                row_length++;
        }
	csvs->row_buffer[row_offset + row_length] = '\n';
	row_length++;

	int parts = (int)ceilf(row_length / (float)fs->buffer_size);
	int left = row_length;

	int no_error = 1;
	for (int p = 0; p < parts; p++) {
		int len = left;
		if (len > fs->buffer_size) {
			len = fs->buffer_size;
		}

		memcpy(fs->buffer, &csvs->row_buffer[row_offset + row_length - left], len);
		if (filestream_write_buffer(fs, len) != 1) {
			no_error = 0;
			printf("error writing row\n");
		}

		left -= len;
	}

	csvs->column_end_index[2 * row] = 0;
	csvs->column_end_index[2 * row + 1] = 0;

	return no_error;
}

void csv_stream_destroy(struct csv_stream *csvs) {
	free(csvs->row_buffer);
	free(csvs->column_end_index);
}

int csv_stream_print_col(struct csv_stream *csvs, int row, int col) {
        int start_index = 0;
        if (col > 0) {
                start_index = csvs->column_end_index[row * csvs->max_column_count + col - 1] + 1;
        }
        int end_index = csvs->column_end_index[row * csvs->max_column_count + col];

        for (int c = start_index; c < end_index; c++) {
                printf("%c", csvs->row_buffer[row * csvs->row_buffer_max_line_length + c]);
        }

        if (end_index == -1) {
                return 0;
        }
        return 1;
}

int csv_stream_print_row(struct csv_stream *csvs, int row) {
        int col = 0;
        while (csv_stream_print_col(csvs, row, col)) {
                printf("\t");
                col++;
        }
        return 1;
}

void csv_stream_print_row_w(struct csv_stream *csvs, int row) {
	char *buffer = (char*) malloc(csvs->row_buffer_max_line_length);
	memcpy(buffer, &csvs->row_buffer[row * csvs->row_buffer_max_line_length], csvs->column_end_index[2 * row]);
	buffer[csvs->column_end_index[2 * row]] = '\0';
	printf("%s\n", buffer);
	free(buffer);
}
