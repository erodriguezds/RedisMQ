#include <stdint.h>
#include <stddef.h>

#define ERRORMSG_EMPTYKEY "EMPTYKEY"

typedef long long mstime_t; /* millisecond time type. */

struct Msg {
    uint64_t id;
    mstime_t created;
    mstime_t lastDelivery; /* Last time the msg was delivered */
    //mstime_t acked; /* When the message was acked */
    uint deliveries; /* how many times the msg has being delivered*/
    //struct Msg *prev;
    struct Msg *next;
    RedisModuleString *value;
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

long long mstime(void);
