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

#ifndef X10_LANG_SHA_H
#define X10_LANG_SHA_H

#include <x10/lang/X10Class.h>
#include <x10aux/sha1alg.h>
#include <x10/lang/Rail.h>

using namespace x10::lang;
using namespace x10aux;

namespace x10 {
    namespace lang {

        class SHA : public ::x10::lang::X10Class {
        public:
            RTT_H_DECLS_CLASS;
    
            virtual ::x10aux::serialization_id_t _get_serialization_id() {
                fprintf(stderr, "SHA cannot be serialized.  (SHA.h)\n");
                abort();
            }
            virtual void _serialize_body(::x10aux::serialization_buffer&) {
                fprintf(stderr, "SHA cannot be serialized.  (SHA.h)\n");
                abort();
            }
            
            SHA() { }
            virtual void _constructor () {  }

            static SHA* _make();
            ~SHA() { }

        public:
            void digest(x10::lang::Rail<signed char>* hash, x10_int offset, x10_int len) {
                sha1.getDigest((unsigned char*)((hash->raw)+offset));
            }

            void update(x10::lang::Rail<signed char>* hash, x10_int offset, x10_int len) {
                char* input = (char*)((hash->raw)+offset);
                sha1.addBytes( input, len );
            }

        private:
            ::x10aux::sha1alg sha1;            
            void raiseException();
        };
    }
}

#endif /* __X10_LANG_SHA_H */

// vim:tabstop=4:shiftwidth=4:expandtab
