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

#include <x10aux/config.h>
#include <x10aux/throw.h>

#include <x10/lang/SHA.h>

#include <x10/lang/IllegalOperationException.h>

#include <errno.h>
#ifdef XRX_DEBUG
#include <iostream>
#endif /* XRX_DEBUG */

using namespace x10::lang;
using namespace x10aux;

SHA* SHA::_make() {
    SHA* this_ = new (x10aux::alloc<SHA>()) SHA();
    return this_;
}

void
SHA::raiseException() {
    throwException<IllegalOperationException>();
}

RTT_CC_DECLS0(SHA, "x10.lang.SHA", RuntimeType::class_kind)

// vim:tabstop=4:shiftwidth=4:expandtab
