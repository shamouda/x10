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

#ifndef X10_AUX_SEML_H
#define X10_AUX_SEML_H

#include <pthread.h>
#include <semaphore.h>

namespace x10aux {

    /**
     * A class that encapsulates the platform-specific details
     * of creating and using a semaphore.
     */
    class semaphore {
    public:
        semaphore(unsigned int permits) { initialize(permits); }
        ~semaphore() { teardown(); }
        
    private:
        void initialize(unsigned int permits);
        void teardown();

    public:

        /**
         * TODOSEM: 
         */
        void acquire();

        /**
         * TODOSEM:
         */
        bool release();

        /**
         * TODOSEM
         */
        bool tryAcquire();

        /**
         * TODOSEM:
         */
        int availablePermits();

    private:
        // semaphore object
        sem_t __sem;
    };
}

#endif /* X10_AUX_SEML_H */

// vim:tabstop=4:shiftwidth=4:expandtab
