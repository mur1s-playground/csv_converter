#include "process.h"
#include "util.h"

#include <stdio.h>
#include <cstring>
#include <ctime>

int process_init(struct process *p, int worker_threads, struct csv_stream *csvs_in, struct filestream *fs_in, int max_rows_per_segment_in, struct csv_stream *csvs_out, struct filestream *fs_out, int max_rows_per_segment_out) {
	p->worker_threads = worker_threads;

	p->csvs_in = csvs_in;
	p->fs_in = fs_in;

	p->csvs_out = csvs_out;
	p->fs_out = fs_out;

	p->thread_workers = (pthread_t *) malloc(worker_threads * sizeof(pthread_t));
	if (p->thread_workers == NULL) {
		printf("Error allocating worker threads\n");
		return 0;
	}

	p->max_rows_per_segment_in = max_rows_per_segment_in;
	p->in_segment_lock_count = csvs_in->row_buffer_row_count / max_rows_per_segment_in;
	p->rows_per_segment_in_to_out = (int *)malloc(p->in_segment_lock_count * sizeof(int));
	p->csvs_in_segment_locks = (pthread_mutex_t *) malloc(p->in_segment_lock_count * sizeof(pthread_mutex_t));

	p->segment_in_to_out_start_row = (int *)malloc(p->in_segment_lock_count * sizeof(int));
	for (int sl = 0; sl < p->in_segment_lock_count; sl++) {
		p->rows_per_segment_in_to_out[sl] = -1;
		p->segment_in_to_out_start_row[sl] = -1;

		pthread_mutex_init(&p->csvs_in_segment_locks[sl], NULL);

		/* prelock for read thread */
		if (sl < p->worker_threads) {
			pthread_mutex_lock(&p->csvs_in_segment_locks[sl]);
		}

		/* prelock for worker thread */
		if (sl > p->in_segment_lock_count - 1 - p->worker_threads) {
			pthread_mutex_lock(&p->csvs_in_segment_locks[sl]);
		}
	}

	p->max_rows_per_segment_out = max_rows_per_segment_out;
	p->out_segment_lock_count = csvs_out->row_buffer_row_count / max_rows_per_segment_out;
	p->csvs_out_segment_locks = (pthread_mutex_t *) malloc(p->out_segment_lock_count * sizeof(pthread_mutex_t));
	for (int sl = 0; sl < p->out_segment_lock_count; sl++) {
		pthread_mutex_init(&p->csvs_out_segment_locks[sl], NULL);

		/* prelock for worker thread */
		if (sl < p->worker_threads) {
			pthread_mutex_lock(&p->csvs_out_segment_locks[sl]);
		}

		/* prelock for write thread */
		if (sl > p->out_segment_lock_count - 1 - p->worker_threads) {
			pthread_mutex_lock(&p->csvs_out_segment_locks[sl]);
		}
	}

	p->done = 0;

	return 1;
}

void *process_read_csv(void *arg) {
	struct process *p = (struct process *) arg;

	int eof = 0;
	int segment = 0;
	int segment_c = 0;

	int new_segment = -1;
	int previous_segment = -1;

	while (!eof) {
		if (segment_c >= p->worker_threads) {
//			printf("read: locking %i\n", segment);
			pthread_mutex_lock(&p->csvs_in_segment_locks[segment]);
//			printf("read: locked %i\n", segment);
			int last_segment = segment - p->worker_threads;
			if (last_segment < 0) last_segment += p->in_segment_lock_count;
			pthread_mutex_unlock(&p->csvs_in_segment_locks[last_segment]);
//			printf("read: unlock %i\n", last_segment);
		}

		if (new_segment != -1) {
			for (int r = new_segment + 1; r < p->max_rows_per_segment_in; r++) {
        	                csv_stream_clear_row(p->csvs_in, segment * p->max_rows_per_segment_in + r);
			}
			int from_row = previous_segment * p->max_rows_per_segment_in + new_segment;
                        int to_row = segment * p->max_rows_per_segment_in;
                        csv_stream_move_row(p->csvs_in, from_row, to_row);
                }

		int r = 0;
		if (new_segment != -1) r = 1;
		new_segment = -1;
		for (; r < p->max_rows_per_segment_in; r++) {
			int fr = csv_stream_get_row(p->csvs_in, p->fs_in);
			if (fr == -2) {
				eof = 1;
			}
			if (r > 0) {
				if (process_is_new_segment(p, segment, r) || eof) {
					new_segment = r + eof;
					int new_out_size = process_segment_size_in_to_out(new_segment);
//					if (eof) { printf("eof: out size %i\n", new_out_size); }

					int previous_out_size = 0;
					if (segment_c > 0) {
						previous_out_size = p->rows_per_segment_in_to_out[previous_segment];
					}
					int total_out_size = previous_out_size + new_out_size;
					p->rows_per_segment_in_to_out[segment] = total_out_size;

					p->segment_in_to_out_start_row[segment] = previous_out_size;
//					printf("%i, previous_out_size %i, total_out_size %i\n", segment, previous_out_size, total_out_size);

					break;
				} else if (r == p->max_rows_per_segment_in - 1) {
					printf("No new segment within expected range\n");
					new_segment = -1;
				}
			}
		}

		previous_segment = segment;
		segment = (segment + 1) % p->in_segment_lock_count;
		if (segment_c < p->worker_threads) segment_c++;
	}
	for (int u = 0; u < p->worker_threads; u++) {
//		pthread_mutex_lock(&p->csvs_in_segment_locks[segment]);
		int last_segment = segment - p->worker_threads;
        	if (last_segment < 0) last_segment += p->in_segment_lock_count;
	        pthread_mutex_unlock(&p->csvs_in_segment_locks[last_segment]);
		segment = (segment + 1) % p->in_segment_lock_count;
	}
//	printf("read quit\n");
	return NULL;
}

void *process_write_csv(void *arg) {
	struct process *p = (struct process *) arg;

	int segment_in = 0;
	int segment = 0;
	int previous_segment = -1;
	int last_start_row = -1;
	while (1) {
//		printf("write: locking %i\n", segment);
		pthread_mutex_lock(&p->csvs_out_segment_locks[segment]);
//		printf("write: locked %i\n", segment);
		int last_segment = segment - p->worker_threads;
		if (last_segment < 0) last_segment += p->out_segment_lock_count;
		pthread_mutex_unlock(&p->csvs_out_segment_locks[last_segment]);
//		printf("write: unlock %i\n", last_segment);

		int start_row = p->segment_in_to_out_start_row[segment_in];
		if (last_start_row > start_row) break;
		last_start_row = start_row;

		int row_count = p->rows_per_segment_in_to_out[segment_in] - start_row;
//		printf("row_count: %i\n", row_count);

		for (int r = 0; r < row_count; r++) {
			int current_row = (start_row + r) % p->csvs_out->row_buffer_row_count;
			csv_stream_write_row(p->csvs_out, p->fs_out, current_row);
		}

		previous_segment = segment;
		segment = (segment + 1) % p->out_segment_lock_count;
		segment_in = (segment_in + 1) % p->in_segment_lock_count;
	}

	fflush(p->fs_out->f_ptr);
//	printf("write: quit\n");
	p->time_end = clock();
	p->done = 1;

	return NULL;
}

void *process_worker(void *arg) {
	struct process_worker_args *pwa = (struct process_worker_args *) arg;
	struct process *p = pwa->p;
	int thread_id = pwa->thread_id;

	int segment = thread_id;

	int segment_out = thread_id;
	int segment_c = 0;
	int last_previous_total = -1;
	while (1) {
//		printf("worker %i: locking %i\n", thread_id, segment);
		pthread_mutex_lock(&p->csvs_in_segment_locks[segment]);
//		printf("worker %i: locked %i\n", thread_id, segment);
		int last_segment = segment - p->worker_threads;
		if (last_segment < 0) last_segment += p->in_segment_lock_count;
		pthread_mutex_unlock(&p->csvs_in_segment_locks[last_segment]);
//		printf("worker %i: unlock %i\n", thread_id, last_segment);

		if (segment_c >= 1) {
//			printf("worker %i: locking w: %i\n", thread_id, segment);
			pthread_mutex_lock(&p->csvs_out_segment_locks[segment_out]);
//			printf("worker %i: locked w: %i\n", thread_id, segment);
			int last_segment_out = segment_out - p->worker_threads;
			if (last_segment_out < 0) last_segment_out += p->out_segment_lock_count;
			pthread_mutex_unlock(&p->csvs_out_segment_locks[last_segment_out]);
//			printf("worker %i: unlock w: %i\n", thread_id, last_segment_out);
		}

		int previous_total_count = p->segment_in_to_out_start_row[segment];
		if (previous_total_count < last_previous_total) break;
		last_previous_total = previous_total_count;
		if (!process_segment(p, segment, segment_out, previous_total_count)) {
			printf("Error processing segment, %i\n", previous_total_count);
			break;
		}

		segment = (segment + p->worker_threads) % p->in_segment_lock_count;
		segment_out = (segment_out + p->worker_threads) % p->out_segment_lock_count;
		if (segment_c < 1) segment_c++;
	}

        pthread_mutex_unlock(&p->csvs_in_segment_locks[segment]);
	pthread_mutex_unlock(&p->csvs_out_segment_locks[segment_out]);

//	printf("worker %i: quit\n", thread_id);
	return NULL;
}

int process_start(struct process *p) {
	pthread_create(&p->thread_fileread, NULL, &process_read_csv, p);
	pthread_create(&p->thread_filewrite, NULL, &process_write_csv, p);

	struct process_worker_args *pwas = (struct process_worker_args *) malloc(p->worker_threads * sizeof(struct process_worker_args));
	for (int w = 0; w < p->worker_threads; w++) {
		pwas[w].p = p;
		pwas[w].thread_id = w;

		pthread_create(&p->thread_workers[w], NULL, &process_worker, &pwas[w]);
	}

	return 1;
}


/* CUSTOMIZE */

int process_segment(struct process *p, int segment_in, int segment_out, int previous_total_count) {
	int segment_in_start_row = p->max_rows_per_segment_in * segment_in;
	int segment_out_start_row = previous_total_count % p->csvs_out->row_buffer_row_count;
	int row_offset_out = segment_out_start_row;


	/* FIRST NAME */
	int first_name_len = 0;
	char *first_name = csv_stream_get_col(p->csvs_in, segment_in_start_row, 0, &first_name_len);

        int first_name_pre_len = 12;
	first_name_len -= first_name_pre_len;
	first_name += first_name_pre_len;

	/* LAST_NAME */
	int last_name_len = 0;
	char *last_name = csv_stream_get_col(p->csvs_in, segment_in_start_row, 1, &last_name_len);

        int last_name_pre_len = 11;
        last_name_len -= last_name_pre_len;
        last_name += last_name_pre_len;

	/* DATE */
	int date_len = 0;
	char *date = csv_stream_get_col(p->csvs_in, segment_in_start_row, 2, &date_len);

        int date_pre_len = 6;
        date_len -= date_pre_len;
        date += date_pre_len;

	for (int row_offset = 3; row_offset < p->max_rows_per_segment_in; row_offset++) {
		/* ROW TYPE */
		int row_type_len = 0;
		char *row_type = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 0, &row_type_len);

		if (row_type_len == 0) {
			break;
		}

		/* ITER NUMBER */
		int iter_number_len = 0;
		char *iter_number = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 1, &iter_number_len);

		/* POWER1 */
		int power1_len = 0;
		char *power1 = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 2, &power1_len);

		/* SPEED1 */
		int speed1_len = 0;
		char *speed1 = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 3, &speed1_len);

		/* SPEED2 */
		int speed2_len = 0;
		char *speed2 = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 4, &speed2_len);

		/* ELECTRICITY */
		int electricity_len = 0;
		char *electricity = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 5, &electricity_len);

		/* EFFORT */
		int effort_len = 0;
		char *effort = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 6, &effort_len);

		/* WEIGHT */
		int weight_len = 0;
		char *weight = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 7, &weight_len);

		/* TORQUE */
		int torque_len = 0;
		char *torque = csv_stream_get_col(p->csvs_in, segment_in_start_row + row_offset, 8, &torque_len);

		if (!first_name_len || !last_name_len || !date_len || !row_type_len || !iter_number_len || !power1_len || !speed1_len || !electricity_len || !effort_len || !weight_len || !torque_len) return 0;

		csv_stream_append_col(p->csvs_out, row_offset_out, first_name, first_name_len);
	        csv_stream_append_col(p->csvs_out, row_offset_out, last_name, last_name_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, date, date_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, row_type, row_type_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, iter_number, iter_number_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, power1, power1_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, speed1, speed1_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, speed2, speed2_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, electricity, electricity_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, effort, effort_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, weight, weight_len);
		csv_stream_append_col(p->csvs_out, row_offset_out, torque, torque_len);

//		csv_stream_print_row_w(p->csvs_out, row_offset_out);

		row_offset_out = (row_offset_out + 1) % p->csvs_out->row_buffer_row_count;
	}

	return 1;
}

int process_is_new_segment(struct process *p, int segment, int row) {
	int segment_in_start_row = p->max_rows_per_segment_in * segment;

	const char *first_name = "first name:";

	int first_column_len = 0;
	char *first_column = csv_stream_get_col(p->csvs_in, segment_in_start_row + row, 0, &first_column_len);

	if (util_str_starts_with(first_column, first_column_len, first_name, strlen(first_name))) {
		return 1;
	}
	return 0;
}

int process_segment_size_in_to_out(int size_in) {
	return size_in - 5;
}

/* ----------- */
