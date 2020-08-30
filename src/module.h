#include <stdint.h>
#include <stddef.h>

#define ERRORMSG_EMPTYKEY "EMPTYKEY"

struct Msg {
    uint64_t id;
    uint64_t delivered;
    //struct Msg *prev;
    struct Msg *next;
    void *value;
};

struct Queue {
    struct Msg *first; /* First to be served */
    struct Msg *last;
    size_t len; /* Number of elements added. */
};

/**
 * Reliable Queue Object 
 */
struct RQueueObject {
    uint64_t nextid;
    struct Queue undelivered;
    struct Queue delivered; /* pending for ACK */
};
