#ifndef X10_LANG_VALRAIL_H
#define X10_LANG_VALRAIL_H


#include <x10aux/config.h>
#include <x10aux/alloc.h>
#include <x10aux/RTT.h>


#include <x10/lang/Fun_0_1.h>

#include <x10aux/rail_utils.h>


namespace x10 {

    namespace lang {

        template<class P1, class R> class Fun_0_1;

        template<class T> class ValRail : public Value,
                                          public virtual Fun_0_1<x10_int,T>,
                                          public x10aux::AnyRail<T>
        {

            public:

            class RTT : public x10aux::RuntimeType {
                public:
                static RTT * const it;

                virtual void init() { initParents(3,x10aux::getRTT<Value>(),
                                                    x10aux::getRTT<Fun_0_1<x10_int,T> >(),
                                                    x10aux::getRTT<Iterable<T> >()); }

                virtual const char *name() const {
                    static const char *name =
                        x10aux::alloc_printf("x10.lang.ValRail[%s]",x10aux::getRTT<T>()->name());
                    return name;
                }
                 
            };

            virtual const x10aux::RuntimeType *_type() const {
                return x10aux::getRTT<ValRail<T> >();
            }

            private:

            ValRail(const ValRail<T>& arr); // disabled

            public:

            ValRail() : x10aux::AnyRail<T>(0) { }
            ValRail(x10_int length_) : x10aux::AnyRail<T>(length_) { }

            ~ValRail() { }

            GPUSAFE T apply(x10_int index) { 
                // do bounds check 
                return apply(index);
            }    

            class Iterator : public Ref, public virtual x10::lang::Iterator<T> {

                protected:

                x10_int i;
                x10aux::ref<ValRail<T> > rail;

                public:

                class RTT : public x10aux::RuntimeType {
                    public:
                    static RTT * const it;

                    virtual void init() {
                       initParents(2,x10aux::getRTT<x10::lang::Iterator<T> >(),
                                     x10aux::getRTT<Ref>());
                    }

                    virtual const char *name() const {
                        static const char *name =
                            x10aux::alloc_printf("x10.lang.ValRail.Iterator[%s]",
                                                 x10aux::getRTT<T>()->name());
                        return name;
                    }

                };
                virtual const x10aux::RuntimeType *_type() const {
                    return x10aux::getRTT<Iterator>();
                }

                Iterator (const x10aux::ref<ValRail> &rail_)
                        : i(0), rail(rail_) { }

                virtual x10_boolean hasNext() {
                    return i < rail->FMGL(length);
                }

                virtual T next() {
                    return (*rail)[i++];
                }

                virtual x10_int hashCode() { return 0; }

                virtual x10aux::ref<String> toString() {
                    return new (x10aux::alloc<String>()) String();
                }

                virtual x10_boolean equals(x10aux::ref<Ref> other) {
                    if (!x10aux::concrete_instanceof<Iterator>(other)) return false;
                    x10aux::ref<Iterator> other_i = other;
                    if (other_i->rail != rail) return false;
                    if (other_i->i != i) return false;
                    return true;
                }

                virtual x10_boolean equals(x10aux::ref<Value> other) {
                    return this->Ref::equals(other);
                }

            };

            virtual x10aux::ref<x10::lang::Iterator<T> > iterator() {
                return new (x10aux::alloc<Iterator>()) Iterator(this);
            }

            virtual x10_boolean _struct_equals(x10aux::ref<Object> other) {
                if (other.get() == this) return true; // short-circuit trivial equality
                if (!this->Value::_struct_equals(other)) return false;
                x10aux::ref<ValRail> other_rail = other;
                // different sizes so false
                if (other_rail->FMGL(length) != this->FMGL(length)) return false;
                if (x10aux::getRTT<T>()->subtypeOf(x10aux::getRTT<Value>())) {
                    // Value type; structurally compare elements
                    for (x10_int i = 0; i < this->FMGL(length); ++i)
                        if (!x10aux::struct_equals((*other_rail)[i], this->raw()[i]))
                            return false;
                } else {
                    // Ref type; simple reference equality
                    for (x10_int i = 0; i < this->FMGL(length); ++i)
                        if ((*other_rail)[i] != this->raw()[i])
                            return false;
                }
                return true;
            }

            virtual x10_int hashCode() { return 0; }

            virtual x10aux::ref<String> toString() {
                return x10aux::AnyRail<T>::toString();
            }

            static x10aux::ref<ValRail<T> > make(x10_int length) {
                x10aux::ref<ValRail<T> > rail = x10aux::alloc_rail<T,ValRail<T> >(length);
                for (x10_int i=0 ; i<length ; ++i) {
                        // Initialise to zero, which should work for
                        // numeric types and x10aux:;ref<T> which I think
                        // covers everything.
                        (*rail)[i] = 0;
                }
                return rail;
            }

            static x10aux::ref<ValRail<T> > make(x10_int length,
                                                 x10aux::ref<Fun_0_1<x10_int,T> > init ) {
                x10aux::ref<ValRail<T> > rail = x10aux::alloc_rail<T,ValRail<T> >(length);
                for (x10_int i=0 ; i<length ; ++i) {
                        (*rail)[i] = init->apply(i);
                }
                return rail;
            }

            static x10aux::ref<ValRail<T> > make(x10aux::ref<ValRail<T> > other) {
                x10_int length = other->FMGL(length);
                x10aux::ref<ValRail<T> > rail = x10aux::alloc_rail<T,ValRail<T> >(length);
                for (x10_int i=0 ; i<length ; ++i) {
                        (*rail)[i] = (*other)[i];
                }
                return rail;
            }

            static const x10aux::serialization_id_t _serialization_id;

            static void _serialize(x10aux::ref<ValRail<T> > this_,
                                   x10aux::serialization_buffer &buf,
                                   x10aux::addr_map &m)
            {
                if (this_ == x10aux::null) {
                    ValRail<T> v;
                    v._serialize_body(buf, m);
                } else {
                    this_->_serialize_body(buf, m);
                }

            }
            void _serialize_id(x10aux::serialization_buffer &buf, x10aux::addr_map &m) {
                buf.write(_serialization_id, m);
            }
            void _serialize_body(x10aux::serialization_buffer &buf, x10aux::addr_map &m) {
                buf.write(this->FMGL(length),m);
                for (x10_int i=0 ; i<this->FMGL(length) ; ++i) {
                    buf.write(this->raw()[i], m); // avoid bounds check
                }
            }
            template<class S> static x10aux::ref<S> _deserialize(x10aux::serialization_buffer &buf)
            {
                x10_int length = buf.read<x10_int>();
                x10aux::ref<ValRail> this_ = x10aux::alloc_rail<T,ValRail<T> >(length);
                for (x10_int i=0 ; i<length ; ++i) {
                    this_->raw()[i] = buf.read<T>(); // avoid bounds check
                }
                return this_;
            }

        };

        template<class T> const x10aux::serialization_id_t ValRail<T>::_serialization_id =
            x10aux::DeserializationDispatcher
                ::addDeserializer(ValRail<T>::template _deserialize<Object>);

        template<class T> typename ValRail<T>::RTT * const ValRail<T>::RTT::it =
            new (x10aux::alloc<typename ValRail<T>::RTT>()) typename ValRail<T>::RTT();

        template<class T> typename ValRail<T>::Iterator::RTT * const
            ValRail<T>::Iterator::RTT::it =
                new (x10aux::alloc<typename ValRail<T>::Iterator::RTT>())
                    typename ValRail<T>::Iterator::RTT();

    }
}


#endif
// vim:tabstop=4:shiftwidth=4:expandtab
