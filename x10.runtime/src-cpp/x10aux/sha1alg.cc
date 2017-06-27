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

#include <x10aux/sha1alg.h>

#include <errno.h>
#include <cstdio>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

// print out memory in hexadecimal
void ::x10aux::sha1alg::hexPrinter( unsigned char* c, int l )
{
    assert( c );
    assert( l > 0 );
    while( l > 0 )
    {
        printf( " %02x", *c );
        l--;
        c++;
    }
}

// circular left bit rotation.  MSB wraps around to LSB
unsigned int ::x10aux::sha1alg::lrot( unsigned int x, int bits )
{
    return (x<<bits) | (x>>(32 - bits));
};

// Save a 32-bit unsigned integer to memory, in big-endian order
void ::x10aux::sha1alg::storeBigEndianUint( unsigned char* byte, unsigned int num )
{
    byte[0] = (unsigned char)(num>>24);
    byte[1] = (unsigned char)(num>>16);
    byte[2] = (unsigned char)(num>>8);
    byte[3] = (unsigned char)num;
}


// Constructor *******************************************************
::x10aux::sha1alg::sha1alg()
{   
    // initialize
    H0 = 0x67452301;
    H1 = 0xefcdab89;
    H2 = 0x98badcfe;
    H3 = 0x10325476;
    H4 = 0xc3d2e1f0;
    unprocessedBytes = 0;
    size = 0;
}

// Destructor ********************************************************
::x10aux::sha1alg::~sha1alg()
{
    // erase data
    H0 = H1 = H2 = H3 = H4 = 0;
    for( int c = 0; c < 64; c++ ) bytes[c] = 0;
    unprocessedBytes = size = 0;
}

// process ***********************************************************
void ::x10aux::sha1alg::process()
{
    int t;
    unsigned int a, b, c, d, e, K, f, W[80];
    // starting values
    a = H0;
    b = H1;
    c = H2;
    d = H3;
    e = H4;
    // copy and expand the message block
    for( t = 0; t < 16; t++ ) W[t] = (bytes[t*4] << 24)
                                    +(bytes[t*4 + 1] << 16)
                                    +(bytes[t*4 + 2] << 8)
                                    + bytes[t*4 + 3];
    for(; t< 80; t++ ) W[t] = lrot( W[t-3]^W[t-8]^W[t-14]^W[t-16], 1 );
    
    /* main loop */
    unsigned int temp;
    for( t = 0; t < 80; t++ )
    {
        if( t < 20 ) {
            K = 0x5a827999;
            f = (b & c) | ((b ^ 0xFFFFFFFF) & d);//TODO: try using ~
        } else if( t < 40 ) {
            K = 0x6ed9eba1;
            f = b ^ c ^ d;
        } else if( t < 60 ) {
            K = 0x8f1bbcdc;
            f = (b & c) | (b & d) | (c & d);
        } else {
            K = 0xca62c1d6;
            f = b ^ c ^ d;
        }
        temp = lrot(a,5) + f + e + W[t] + K;
        e = d;
        d = c;
        c = lrot(b,30);
        b = a;
        a = temp;
        //printf( "t=%d %08x %08x %08x %08x %08x\n",t,a,b,c,d,e );
    }
    /* add variables */
    H0 += a;
    H1 += b;
    H2 += c;
    H3 += d;
    H4 += e;
    //printf( "Current: %08x %08x %08x %08x %08x\n",H0,H1,H2,H3,H4 );
    /* all bytes have been processed */
    unprocessedBytes = 0;
}

// addBytes **********************************************************
void ::x10aux::sha1alg::addBytes( const char* data, int num )
{
    // add these bytes to the running total
    size += num;
    // repeat until all data is processed
    while( num > 0 )
    {
        // number of bytes required to complete block
        int needed = 64 - unprocessedBytes;
        // number of bytes to copy (use smaller of two)
        int toCopy = (num < needed) ? num : needed;
        // Copy the bytes
        memcpy( bytes + unprocessedBytes, data, toCopy );
        // Bytes have been copied
        num -= toCopy;
        data += toCopy;
        unprocessedBytes += toCopy;
        
        // there is a full block
        if( unprocessedBytes == 64 ) process();
    }
}

// digest ************************************************************
void ::x10aux::sha1alg::getDigest(unsigned char* digest)
{
    // save the message size
    unsigned int totalBitsL = size << 3;
    unsigned int totalBitsH = size >> 29;
    // add 0x80 to the message
    addBytes( "\x80", 1 );
    
    unsigned char footer[64] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    // block has no room for 8-byte filesize, so finish it
    if( unprocessedBytes > 56 )
        addBytes( (char*)footer, 64 - unprocessedBytes);

    // how many zeros do we need
    int neededZeros = 56 - unprocessedBytes;
    // store file size (in bits) in big-endian format
    storeBigEndianUint( footer + neededZeros    , totalBitsH );
    storeBigEndianUint( footer + neededZeros + 4, totalBitsL );
    // finish the final block
    addBytes( (char*)footer, neededZeros + 8 );
    // allocate memory for the digest bytes

    // copy the digest bytes
    storeBigEndianUint( digest, H0 );
    storeBigEndianUint( digest + 4, H1 );
    storeBigEndianUint( digest + 8, H2 );
    storeBigEndianUint( digest + 12, H3 );
    storeBigEndianUint( digest + 16, H4 );
    
    // initialize
    H0 = 0x67452301;
    H1 = 0xefcdab89;
    H2 = 0x98badcfe;
    H3 = 0x10325476;
    H4 = 0xc3d2e1f0;
    unprocessedBytes = 0;
    size = 0;
    for( int c = 0; c < 64; c++ ) bytes[c] = 0;
}

// vim:tabstop=4:shiftwidth=4:expandtab
