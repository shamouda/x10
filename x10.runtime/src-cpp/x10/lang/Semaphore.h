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

#ifndef X10_LANG_SEMAPHORE_H
#define X10_LANG_SEMAPHORE_H

#include <x10/lang/X10Class.h>

#include <x10aux/sem.h>

namespace x10 {
    namespace lang {

       /**
        * A low-level lock that provides a subset of the functionality
        * of java.util.concurrent.Semaphore.
        * 
        * TODOSEM: describe the semapahore class
        */
        class Semaphore : public ::x10::lang::X10Class {
        public:
            RTT_H_DECLS_CLASS;
    
            virtual ::x10aux::serialization_id_t _get_serialization_id() {
                fprintf(stderr, "Semaphore cannot be serialized.  (Semaphore.h)\n");
                abort();
            }
            virtual void _serialize_body(::x10aux::serialization_buffer&) {
                fprintf(stderr, "Semaphore cannot be serialized.  (Semaphore.h)\n");
                abort();
            }

            Semaphore() : _sem(1) { }
            virtual void _constructor () {  }
            
            
			Semaphore(x10_int permits) : _sem(permits) { }
            virtual void _constructor (x10_int permits) {  }

            static Semaphore* _make(x10_int permits);
            ~Semaphore() { }

        public:
           /**
            * TODOSEM: describe the semapahore class
            */
            void acquire() { _sem.acquire(); }


            /**
             * TODOSEM: describe the semapahore class
             */
            void release() { if (!_sem.release()) raiseException(); }
                

            /**
             * TODOSEM: describe the semapahore class
             */
            x10_boolean tryAcquire() { return _sem.tryAcquire(); }

            /**
             * TODOSEM: describe the semapahore class
             */
            x10_int availablePermits() { return _sem.availablePermits(); }

        private:
            ::x10aux::semaphore _sem;            
            void raiseException();
        };
    }
}

#endif /* __X10_LANG_SEMAPHORE_H */

// vim:tabstop=4:shiftwidth=4:expandtab
