#ifndef HASH_INTERFACE_H_
#define HASH_INTERFACE_H_

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#include "util/pair.h"
#include "util/timer.h"

class Hash {
  public:
    Hash(void) = default;
    ~Hash(void) = default;

    virtual void Insert(Key_t&, Value_t) = 0;


    Timer timer;
    double breakdown = 0;
};


#endif  // _HASH_INTERFACE_H_
