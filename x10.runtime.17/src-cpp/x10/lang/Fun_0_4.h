#ifndef X10_LANG_FUN_0_4_H
#define X10_LANG_FUN_0_4_H

#include <x10aux/config.h>
#include <x10aux/RTT.h>
#include <x10/lang/Object.h>

namespace x10 {
    namespace lang {

        extern const x10aux::RuntimeType* _initRTTHelper_Fun_0_4(const x10aux::RuntimeType **location,
                                                                 const x10aux::RuntimeType *rtt0,
                                                                 const x10aux::RuntimeType *rtt1,
                                                                 const x10aux::RuntimeType *rtt2,
                                                                 const x10aux::RuntimeType *rtt3,
                                                                 const x10aux::RuntimeType *rtt4);

        template<class P1, class P2, class P3, class P4, class R> class Fun_0_4 : public virtual Object {
            public:
            static const x10aux::RuntimeType* rtt;
            static const x10aux::RuntimeType* getRTT() { return NULL == rtt ? _initRTT() : rtt; }
            static const x10aux::RuntimeType* _initRTT() {
                return x10::lang::_initRTTHelper_Fun_0_4(&rtt, x10aux::getRTT<P1>(), x10aux::getRTT<P2>(), 
                                                         x10aux::getRTT<P3>(), x10aux::getRTT<P4>(), 
                                                         x10aux::getRTT<R>());
            }
            virtual const x10aux::RuntimeType *_type() const { return getRTT(); }

            virtual ~Fun_0_4() { };
            virtual R apply(P1 p1, P2 p2, P3 p3, P4 p4) = 0;
        };
        template<class P1, class P2, class P3, class P4, class R>
            const x10aux::RuntimeType* Fun_0_4<P1,P2,P3,P4,R>::rtt = NULL;
    }
}
#endif
// vim:tabstop=4:shiftwidth=4:expandtab
