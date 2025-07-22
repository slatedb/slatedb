#include <stdint.h>
#include "_cgo_export.h"

void kv_trampoline(const unsigned char* key_ptr,
                   unsigned int key_len,
                   const unsigned char* val_ptr,
                   unsigned int val_len,
                   void* userdata) {
    goKvCollector((unsigned char*)key_ptr, key_len, (unsigned char*)val_ptr, val_len, userdata);
} 