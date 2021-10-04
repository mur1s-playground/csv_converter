#include "csv_stream.h"
#include "filestream.h"
#include "process.h"

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <ctime>

#include <pthread.h>

using namespace std;

int main() {
	size_t time_start = clock();

	/* SYSTEM */
	int worker_threads = 4;

	/* ----- */
	/* INPUT */
	/* ----- */

	/* PROCESS */
        int max_rows_per_segment_in = 30;

	/* CSV_STREAM CONFIG */
	char csv_delimiter = ',';
	char csv_string_boundary = '"';
	enum eol_type eol_t = EOL_TYPE_N;
	int row_buffer_row_count = max_rows_per_segment_in * 50;
	int row_buffer_max_line_length = 400;
	int max_column_count = 40;

	/* FILESTREAM CFG  */
        string filename("data/data_cleaning_challenge.csv");
        int buffer_size = 2048;


	/* ------ */
	/* OUTPUT */
	/* ------ */

	/* PROCESS */
        int max_rows_per_segment_out = 25;

	/* CSV_STREAM CONFIG */
	char csv_delimiter_out = ',';
	char csv_string_boundary_out = '"';
	enum eol_type eol_t_out = EOL_TYPE_N;
	int row_buffer_row_count_out = max_rows_per_segment_out * 100;
	int row_buffer_max_line_length_out = 1024;
	int max_column_count_out = 14;

	enum csv_column_type csv_col_t_out[14];
	for (int ct = 0; ct < 14; ct++) {
		csv_col_t_out[ct] = CSV_COLUMN_TYPE_UNBOUNDED_STRING;
	}

	/* FILESTREAM CFG */
	string filename_out("data/data_cleaning_challenge_out.csv");
	int buffer_size_out = 2048;

        /* -------------- */


	struct process p;

	struct csv_stream csvs;
	csv_stream_init(&csvs, csv_delimiter, csv_string_boundary, eol_t, row_buffer_row_count, row_buffer_max_line_length, max_column_count);

	struct csv_stream csvs_out;
	csv_stream_init(&csvs_out, csv_delimiter_out, csv_string_boundary_out, eol_t_out, row_buffer_row_count_out, row_buffer_max_line_length_out, max_column_count_out, csv_col_t_out);

	struct filestream fs;
	if (filestream_open(&fs, filename, buffer_size, "r")) {

		struct filestream fs_out;
		if (filestream_open(&fs_out, filename_out, buffer_size_out, "w")) {
			process_init(&p, worker_threads, &csvs, &fs, max_rows_per_segment_in, &csvs_out, &fs_out, max_rows_per_segment_out);
			process_start(&p);

			while (true) {
				sleep(1);
				if (p.done) break;
			}

			filestream_close(&fs_out);
		}

		filestream_close(&fs);
	}
	csv_stream_destroy(&csvs);

	printf("time: %f\n", ((p.time_end - time_start) / (double) CLOCKS_PER_SEC));
	return 0;
}
