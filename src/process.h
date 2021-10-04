#ifndef PROCESS_H
#define PROCESS_H

#include "csv_stream.h"
#include "filestream.h"

#include <pthread.h>

struct process_worker_args {
	struct process *p;
	int thread_id;
};

struct process {
	size_t time_end;

	int worker_threads;

	struct csv_stream *csvs_in;
	struct filestream *fs_in;
	int max_rows_per_segment_in;
	int *rows_per_segment_in_to_out;
	int in_segment_lock_count;
	pthread_mutex_t *csvs_in_segment_locks;
	pthread_mutex_t csvs_in_status_vars_lock;

	int *segment_in_to_out_start_row;

	struct csv_stream *csvs_out;
	struct filestream *fs_out;
	int max_rows_per_segment_out;
	int out_segment_lock_count;
	pthread_mutex_t *csvs_out_segment_locks;

	pthread_t thread_fileread;
	pthread_t thread_filewrite;
	pthread_t *thread_workers;

	int done;
};

int process_init(struct process *p, int worker_threads, struct csv_stream *csvs_in, struct filestream *fs_in, int max_rows_per_segment_in, struct csv_stream *csvs_out, struct filestream *fs_out, int max_rows_per_segment_out);

void *process_read_csv(void *p);
void *process_write_csv(void *p);
void *process_worker(void *pwa);

int process_start(struct process *p);

int process_segment(struct process *p, int segment_in, int segment_out, int previous_total_count);
int process_is_new_segment(struct process *p, int segment, int row);
int process_segment_size_in_to_out(int size_in);

#endif
