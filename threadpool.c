#include "threadpool.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

static void *Thread_run(void *arg);

ThreadPool_t *ThreadPool_create(unsigned int num) {
    ThreadPool_t *tp = (ThreadPool_t *)malloc(sizeof(ThreadPool_t));
    if (!tp) return NULL;

    tp->threads = (pthread_t *)malloc(num * sizeof(pthread_t));
    tp->num_threads = num;
    tp->jobs.size = 0;
    tp->jobs.head = tp->jobs.tail = NULL;
    tp->stop = false;

    pthread_mutex_init(&tp->jobs.mutex, NULL);
    pthread_cond_init(&tp->jobs.cond, NULL);

    for (unsigned int i = 0; i < num; i++) {
        pthread_create(&tp->threads[i], NULL, Thread_run, tp);
    }

    return tp;
}


void ThreadPool_destroy(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);
    tp->stop = true;
    pthread_cond_broadcast(&tp->jobs.cond);
    pthread_mutex_unlock(&tp->jobs.mutex);

    for (unsigned int i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }

    pthread_mutex_destroy(&tp->jobs.mutex);
    pthread_cond_destroy(&tp->jobs.cond);
    free(tp->threads);
    free(tp);
}


bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    ThreadPool_job_t *job = (ThreadPool_job_t *)malloc(sizeof(ThreadPool_job_t));
    if (!job) return false;

    job->func = func;
    job->arg = arg;
    job->next = NULL;

    pthread_mutex_lock(&tp->jobs.mutex);
    
    if (tp->jobs.tail) {
        tp->jobs.tail->next = job;
    } else {
        tp->jobs.head = job;
    }
    tp->jobs.tail = job;
    tp->jobs.size++;

    if (tp->jobs.size > 0) {
        pthread_cond_signal(&tp->jobs.cond);
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    return true;
}



ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    while (tp->jobs.size == 0 && !tp->stop) {
        pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
    }

    if (tp->stop) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return NULL;
    }

    ThreadPool_job_t *job = tp->jobs.head;
    if (job) {
        tp->jobs.head = job->next;
        if (!tp->jobs.head) {
            tp->jobs.tail = NULL;
        }
        tp->jobs.size--;
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    return job;
}


static void *Thread_run(void *arg) {
    ThreadPool_t *tp = (ThreadPool_t *)arg;

    while (1) {
        ThreadPool_job_t *job = ThreadPool_get_job(tp);
        if (!job) break;

        job->func(job->arg);
        free(job);
    }

    return NULL;
}


void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    while (tp->jobs.size > 0) {
        pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
}


