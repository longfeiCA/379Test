#include "mapreduce.h"
#include "threadpool.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

// Define partition structure and global variables
typedef struct KeyValue {
    char *key;
    char *value;
    struct KeyValue *next;
} KeyValue;

typedef struct Partition {
    KeyValue *head;         // Head of the list for key-value pairs
    pthread_mutex_t mutex;  // Mutex for thread-safe access
} Partition;

static Partition *partitions;      // Array of partitions
static unsigned int num_partitions; // Total partitions
static Reducer global_reducer;

void MR_Run(unsigned int file_count, char *file_names[], Mapper mapper, Reducer reducer, 
            unsigned int num_workers, unsigned int num_parts) {
    global_reducer = reducer;
    // Initialize partitions and mutexes
    num_partitions = num_parts;
    partitions = (Partition *)malloc(num_parts * sizeof(Partition));
    for (unsigned int i = 0; i < num_parts; i++) {
        partitions[i].head = NULL;
        pthread_mutex_init(&partitions[i].mutex, NULL);
    }

    // Initialize thread pool
    ThreadPool_t *thread_pool = ThreadPool_create(num_workers);

    // Map phase: Assign each file to a thread in the thread pool
    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(thread_pool, (thread_func_t)mapper, file_names[i]);
    }

    // Wait until all map jobs complete
    ThreadPool_check(thread_pool);

    // Reduce phase: Assign each partition to a thread in the thread pool
    for (unsigned int i = 0; i < num_parts; i++) {
        ThreadPool_add_job(thread_pool, (thread_func_t)MR_Reduce, (void *)(intptr_t)i);
    }

    // Wait for all reduce jobs to complete
    ThreadPool_check(thread_pool);

    // Cleanup
    ThreadPool_destroy(thread_pool);
    for (unsigned int i = 0; i < num_parts; i++) {
        pthread_mutex_destroy(&partitions[i].mutex);
        // Free partition data if necessary
    }
    free(partitions);
}

void MR_Emit(char *key, char *value) {
    unsigned int partition_idx = MR_Partitioner(key, num_partitions);

    // Lock the partition mutex
    pthread_mutex_lock(&partitions[partition_idx].mutex);

    // Create a new KeyValue node
    KeyValue *kv = (KeyValue *)malloc(sizeof(KeyValue));
    kv->key = strdup(key);
    kv->value = strdup(value);
    kv->next = partitions[partition_idx].head;
    partitions[partition_idx].head = kv;

    // Unlock the partition mutex
    pthread_mutex_unlock(&partitions[partition_idx].mutex);
}

unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % num_partitions;
}

void MR_Reduce(void *threadarg) {
    unsigned int partition_idx = (unsigned int)(intptr_t)threadarg;

    pthread_mutex_lock(&partitions[partition_idx].mutex);

    KeyValue *current = partitions[partition_idx].head;
    while (current) {
        char *key = current->key;
        global_reducer(key, partition_idx); // Use the global reducer pointer

        while (current && strcmp(current->key, key) == 0) {
            current = current->next;
        }
    }

    pthread_mutex_unlock(&partitions[partition_idx].mutex);
}


char *MR_GetNext(char *key, unsigned int partition_idx) {
    pthread_mutex_lock(&partitions[partition_idx].mutex);
    KeyValue *current = partitions[partition_idx].head;

    while (current && strcmp(current->key, key) != 0) {
        current = current->next;
    }

    char *value = NULL;
    if (current && strcmp(current->key, key) == 0) {
        value = current->value;
    }

    pthread_mutex_unlock(&partitions[partition_idx].mutex);
    return value;
}
