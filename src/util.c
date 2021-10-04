#include "util.h"

int util_str_starts_with(const char *str, int str_size, const char *search, int search_size) {
	if (str_size < search_size) return 0;
	for (int c = 0; c < search_size; c++) {
		if (str[c] != search[c]) return 0;
	}
	return 1;
}
