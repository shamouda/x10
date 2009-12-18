#ifndef X10AUX_DESERIALIZATION_DISPATCHER_H
#define X10AUX_DESERIALIZATION_DISPATCHER_H

#include <x10aux/config.h>

#include <x10aux/ref.h>
#include <x10aux/network.h>

namespace x10 { namespace lang { class Ref; } }

namespace x10aux {

    class deserialization_buffer;

    typedef ref<x10::lang::Ref> (*Deserializer)(deserialization_buffer &buf);
    template<> inline const char *typeName<Deserializer>() { return "Deserializer"; }

    typedef x10_ulong (*CUDAPre)(deserialization_buffer &buf, place p,
                                           size_t &blocks, size_t &threads, size_t &shm);
    template<> inline const char *typeName<CUDAPre>() { return "CUDAPre"; }

    typedef void *(*BufferFinder)(deserialization_buffer &buf, x10_int len);
    template<> inline const char *typeName<BufferFinder>() { return "BufferFinder"; }

    typedef void (*Notifier)(deserialization_buffer &buf, x10_int len);
    template<> inline const char *typeName<Notifier>() { return "Notifier"; }

    template<> inline const char *typeName<serialization_id_t>() { return "serialization_id_t"; }

    class DeserializationDispatcher {
        protected:
        static DeserializationDispatcher *it;

        public:
        struct Data {
            BufferFinder put_bfinder;
            Notifier put_notifier;
            BufferFinder get_bfinder;
            Notifier get_notifier;
            BufferFinder cuda_put_bfinder;
            Notifier cuda_put_notifier;
            BufferFinder cuda_get_bfinder;
            Notifier cuda_get_notifier;

            Deserializer deser;
            CUDAPre cuda_pre;
            const char *cubin;
            const char *kernel;

            bool has_mt;
            x10aux::msg_type mt;
            x10aux::serialization_id_t sid;
        };
        protected:

        Data *data_v;
        size_t data_c;
        size_t next_id;

        public:
        DeserializationDispatcher () : data_v(NULL), data_c(0), next_id(1) { }
        ~DeserializationDispatcher () {
            ::free(data_v); // do not use GC
        }

        
        template<class T> static ref<T> create(deserialization_buffer &buf);
        template<class T> static ref<T> create(deserialization_buffer &buf,
                                               serialization_id_t id);

        ref<x10::lang::Ref> create_(deserialization_buffer &buf);
        ref<x10::lang::Ref> create_(deserialization_buffer &buf, serialization_id_t id);

        static serialization_id_t addDeserializer (Deserializer deser, bool is_async=false,
                                                   CUDAPre cuda_pre = NULL,
                                                   const char *cubin = NULL,
                                                   const char *kernel = NULL);
        serialization_id_t addDeserializer_ (Deserializer deser, bool is_async,
                                             CUDAPre cuda_pre,
                                             const char *cubin, const char *kernel);

        static CUDAPre getCUDAPre(serialization_id_t id);
        CUDAPre getCUDAPre_(serialization_id_t id);

        static serialization_id_t addPutFunctions(BufferFinder bfinder, Notifier notifier,
                                                 BufferFinder cuda_bfinder, Notifier cuda_notifier);
        serialization_id_t addPutFunctions_(BufferFinder bfinder, Notifier notifier,
                                            BufferFinder cuda_bfinder, Notifier cuda_notifier);
        static BufferFinder getPutBufferFinder(serialization_id_t id);
        BufferFinder getPutBufferFinder_(serialization_id_t id);
        static Notifier getPutNotifier(serialization_id_t id);
        Notifier getPutNotifier_(serialization_id_t id);
        static BufferFinder getCUDAPutBufferFinder(serialization_id_t id);
        BufferFinder getCUDAPutBufferFinder_(serialization_id_t id);
        static Notifier getCUDAPutNotifier(serialization_id_t id);
        Notifier getCUDAPutNotifier_(serialization_id_t id);

        static serialization_id_t addGetFunctions(BufferFinder bfinder, Notifier notifier,
                                                 BufferFinder cuda_bfinder, Notifier cuda_notifier);
        serialization_id_t addGetFunctions_(BufferFinder bfinder, Notifier notifier,
                                            BufferFinder cuda_bfinder, Notifier cuda_notifier);
        static BufferFinder getGetBufferFinder(serialization_id_t id);
        BufferFinder getGetBufferFinder_(serialization_id_t id);
        static Notifier getGetNotifier(serialization_id_t id);
        Notifier getGetNotifier_(serialization_id_t id);
        static BufferFinder getCUDAGetBufferFinder(serialization_id_t id);
        BufferFinder getCUDAGetBufferFinder_(serialization_id_t id);
        static Notifier getCUDAGetNotifier(serialization_id_t id);
        Notifier getCUDAGetNotifier_(serialization_id_t id);

        static x10aux::msg_type getMsgType(serialization_id_t id);
        x10aux::msg_type getMsgType_(serialization_id_t id);

        static serialization_id_t getSerializationId(x10aux::msg_type id);
        serialization_id_t getSerializationId_(x10aux::msg_type id);

        static void registerHandlers(void);
        void registerHandlers_(void);
    };

    inline CUDAPre DeserializationDispatcher::getCUDAPre (serialization_id_t id)
    { return it->getCUDAPre_(id); }


    inline BufferFinder DeserializationDispatcher::getPutBufferFinder (serialization_id_t id)
    { return it->getPutBufferFinder_(id); }

    inline BufferFinder DeserializationDispatcher::getGetBufferFinder (serialization_id_t id)
    { return it->getGetBufferFinder_(id); }

    inline Notifier DeserializationDispatcher::getPutNotifier (serialization_id_t id)
    { return it->getPutNotifier_(id); }

    inline Notifier DeserializationDispatcher::getGetNotifier (serialization_id_t id)
    { return it->getGetNotifier_(id); }

    inline BufferFinder DeserializationDispatcher::getCUDAPutBufferFinder (serialization_id_t id)
    { return it->getCUDAPutBufferFinder_(id); }

    inline BufferFinder DeserializationDispatcher::getCUDAGetBufferFinder (serialization_id_t id)
    { return it->getCUDAGetBufferFinder_(id); }

    inline Notifier DeserializationDispatcher::getCUDAPutNotifier (serialization_id_t id)
    { return it->getCUDAPutNotifier_(id); }

    inline Notifier DeserializationDispatcher::getCUDAGetNotifier (serialization_id_t id)
    { return it->getCUDAGetNotifier_(id); }


    inline x10aux::msg_type DeserializationDispatcher::getMsgType (serialization_id_t id)
    { return it->getMsgType_(id); }

    inline serialization_id_t DeserializationDispatcher::getSerializationId (x10aux::msg_type id)
    { return it->getSerializationId_(id); }


    template<class T> ref<T> DeserializationDispatcher::create(deserialization_buffer &buf,
                                                               serialization_id_t id)
    { return static_cast<ref<T> >(it->create_(buf,id)); }

    template<class T> ref<T> DeserializationDispatcher::create(deserialization_buffer &buf)
    { return static_cast<ref<T> >(it->create_(buf)); }
    
    template<> inline const char *typeName<DeserializationDispatcher>()
    { return "DeserializationDispatcher"; }
}

#endif
// vim:tabstop=4:shiftwidth=4:expandtab
