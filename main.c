#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Mapper function: Emits each word with a count of 1
void Mapper(char *file_name) {
    FILE *file = fopen(file_name, "r");
    if (!file) {
        perror("Error opening file");
        return;
    }

    char line[1024];
    while (fgets(line, sizeof(line), file)) {
        char *word = strtok(line, " \t\n");
        while (word) {
            MR_Emit(word, "1");
            word = strtok(NULL, " \t\n");
        }
    }

    fclose(file);
}

// Reducer function: Sums up counts for each word and prints the result
void Reducer(char *key, unsigned int partition_idx) {
    int count = 0;
    char *value;

    while ((value = MR_GetNext(key, partition_idx)) != NULL) {
        count += atoi(value);
    }

    printf("%s %d\n", key, count);
}

// Main function to set up and run the MapReduce
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <input_files...>\n", argv[0]);
        exit(1);
    }

    // Define the number of workers and partitions
    unsigned int num_workers = 4;
    unsigned int num_partitions = 5;

    // Start the MapReduce execution
    MR_Run(argc - 1, &argv[1], Mapper, Reducer, num_workers, num_partitions);

    return 0;
}

