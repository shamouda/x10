/*
 *
 *  Copyright (c) 2005 Michael D. Leonhard
 *  http://tamale.net/
 *  Permission is hereby granted, free of charge, to any person obtaining a copy of
 *  this software and associated documentation files (the "Software"), to deal in
 *  the Software without restriction, including without limitation the rights to
 *  use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 *  of the Software, and to permit persons to whom the Software is furnished to do
 *  so, subject to the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.

 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

#ifndef X10_AUX_SHAALGL_H
#define X10_AUX_SHAALGL_H

namespace x10aux {

    class sha1alg {
    private:
        // fields
        unsigned int H0, H1, H2, H3, H4;
        unsigned char bytes[64];
        int unprocessedBytes;
        unsigned int size;
        void process();
    public:
        sha1alg();
        ~sha1alg();
        void addBytes( const char* data, int num );
        void getDigest(unsigned char* bytes);
        // utility methods
        static unsigned int lrot( unsigned int x, int bits );
        static void storeBigEndianUint( unsigned char* byte, unsigned int num );
        static void hexPrinter( unsigned char* c, int l );
    };
}

#endif /* X10_AUX_SHAALGL_H */

// vim:tabstop=4:shiftwidth=4:expandtab
