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

/* Copyright (c) 2005 Michael D. Leonhard   
http://tamale.net/
This file is licensed under the terms described in the
accompanying LICENSE file.
*/

#ifndef X10_AUX_SHAALGL_H
#define X10_AUX_SHAALGL_H

typedef unsigned int Uint32;

namespace x10aux {

    class sha1alg {
    private:
        // fields
        Uint32 H0, H1, H2, H3, H4;
        unsigned char bytes[64];
        int unprocessedBytes;
        Uint32 size;
        void process();
    public:
        sha1alg();
        ~sha1alg();
        void addBytes( const char* data, int num );
        unsigned char* getDigest();
        // utility methods
        static Uint32 lrot( Uint32 x, int bits );
        static void storeBigEndianUint32( unsigned char* byte, Uint32 num );
        static void hexPrinter( unsigned char* c, int l );
    };
}

#endif /* X10_AUX_SHAALGL_H */

// vim:tabstop=4:shiftwidth=4:expandtab
