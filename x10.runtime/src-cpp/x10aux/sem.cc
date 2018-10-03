/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

#include <x10aux/sem.h>

#include <errno.h>
#include <cstdio>

void x10aux::semaphore::initialize(unsigned int permits) {
    sem_init(&__sem, PTHREAD_PROCESS_PRIVATE, permits);    
}

// destructor
void x10aux::semaphore::teardown() {
    // free semaphore object
    sem_destroy(&__sem);
}

// semaphore wait (blocking)
void x10aux::semaphore::acquire() {
    // blocks until the lock becomes available
    sem_wait(&__sem);
}

// release lock
bool x10aux::semaphore::release() {
	int err = sem_post(&__sem);
	int errsv = errno;
    if ( err != 0) {
    	printf("sem_error %d  %d \n", err, errsv );
        return false;
    }
    return true;
}

// acquire semaphore (non-blocking)
bool x10aux::semaphore::tryAcquire() {
    // acquire the lock only if it is not held by another thread
    if (sem_trywait(&__sem) == 0) {
        return true;
    }
    return false;
}

// get lock count
int x10aux::semaphore::availablePermits() {
    int v = 0;
    sem_getvalue (&__sem, &v);
    return v;
}

// vim:tabstop=4:shiftwidth=4:expandtab
