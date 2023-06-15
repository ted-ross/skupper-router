/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// obtain access to pthread_setname_np
#define _GNU_SOURCE

//
// Enable debug for asserts in this module regardless of what the project-wide
// setting is.
//
#undef NDEBUG

#include "qpid/dispatch/threading.h"

#include "qpid/dispatch/ctools.h"

#include <assert.h>
#include <pthread.h>


void sys_mutex_init(sys_mutex_t *mutex)
{
    int result = pthread_mutex_init(&(mutex->mutex), 0);
    (void) result; assert(result == 0);
}


void sys_mutex_free(sys_mutex_t *mutex)
{
    int result = pthread_mutex_destroy(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_mutex_lock(sys_mutex_t *mutex)
{
    int result = pthread_mutex_lock(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_mutex_unlock(sys_mutex_t *mutex)
{
    int result = pthread_mutex_unlock(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_cond_init(sys_cond_t *cond)
{
    int result = pthread_cond_init(&(cond->cond), 0);
    (void) result; assert(result == 0);
}


void sys_cond_free(sys_cond_t *cond)
{
    int result = pthread_cond_destroy(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_cond_wait(sys_cond_t *cond, sys_mutex_t *held_mutex)
{
    int result = pthread_cond_wait(&(cond->cond), &(held_mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_cond_signal(sys_cond_t *cond)
{
    int result = pthread_cond_signal(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_cond_signal_all(sys_cond_t *cond)
{
    int result = pthread_cond_broadcast(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_rwlock_init(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_init(&(lock->lock), 0);
    (void) result; assert(result == 0);
}


void sys_rwlock_free(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_destroy(&(lock->lock));
    (void) result; assert(result == 0);
}


void sys_rwlock_wrlock(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_wrlock(&(lock->lock));
    assert(result == 0);
}


void sys_rwlock_rdlock(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_rdlock(&(lock->lock));
    assert(result == 0);
}


void sys_rwlock_unlock(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_unlock(&(lock->lock));
    assert(result == 0);
}


void sys_spinlock_init(sys_spinlock_t *lock)
{
    int result = pthread_spin_init(&(lock->lock), PTHREAD_PROCESS_PRIVATE);
    assert(result == 0);
}


void sys_spinlock_free(sys_spinlock_t *lock)
{
    int result = pthread_spin_destroy(&(lock->lock));
    assert(result == 0);
}


void sys_spinlock_lock(sys_spinlock_t *lock)
{
    int result = pthread_spin_lock(&(lock->lock));
    assert(result == 0);
}


void sys_spinlock_unlock(sys_spinlock_t *lock)
{
    int result = pthread_spin_unlock(&(lock->lock));
    assert(result == 0);
}



struct sys_thread_t {
    pthread_t thread;
    void *(*f)(void *);
    void *arg;
    char  name[16];
};

// initialize the per-thread _self to a non-zero value.  This dummy value will
// be returned when sys_thread_self() is called from the process's main thread
// of execution (which is not a pthread).  Using a non-zero value provides a
// way to distinguish a thread id from a zero (unset) value.
//
static sys_thread_t           _main_thread_id = {.name = "main"};
static __thread sys_thread_t *_self           = &_main_thread_id;

// bootstrap _self before calling thread's main function
//
static void *_thread_init(void *arg)
{
    _self = (sys_thread_t*) arg;
    return _self->f(_self->arg);
}


sys_thread_t *sys_thread(const char *thread_name, void *(*run_function) (void *), void *arg)
{
    // thread name may be at most 16 bytes, counting the final \0 byte
    assert(strlen(thread_name) < 16);
    sys_thread_t *thread = NEW(sys_thread_t);
    thread->f = run_function;
    thread->arg = arg;
    strcpy(thread->name, thread_name);
    pthread_create(&(thread->thread), 0, _thread_init, (void*) thread);
    pthread_setname_np(thread->thread, thread->name);
    return thread;
}

// note: called by signal handler, do not invoke async signal unsafe code in this function!  See man signal(7)
sys_thread_t *sys_thread_self(void)
{
    return _self;
}

// note: called by signal handler, do not invoke async signal unsafe code in this function!  See man signal(7)
const char *sys_thread_name(sys_thread_t *thread)
{
    if (thread == 0)
        thread = _self;
    return thread->name;
}

void sys_thread_free(sys_thread_t *thread)
{
    assert(thread != &_main_thread_id);
    free(thread);
}


void sys_thread_join(sys_thread_t *thread)
{
    assert(thread != &_main_thread_id);
    pthread_join(thread->thread, 0);
}
