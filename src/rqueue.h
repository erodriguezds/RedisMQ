#include <stdint.h>
#include <sys/time.h>
#include "../redismodule.h"

#define MSG_ID_FORMAT "%lu-%lu"

typedef long long mstime_t; /* millisecond time type. */

/* Queue item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
struct MsgID {
    uint64_t ms;        /* Unix time in milliseconds. */
    uint64_t seq;       /* Sequence number. */
};

struct Msg {
    struct MsgID id;
    uint deliveries; /* how many times the msg has being delivered*/
    mstime_t lastDelivery; /* Last time the msg was delivered */
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
typedef struct rqueue_t {
    struct MsgID last_id;       /* Zero if there are yet no items. */
    struct Queue undelivered;
    struct Queue delivered; /* Queue of messages that has being delivered at-least-one */
} rqueue_t;

long long mstime(void);

/* Return the UNIX time in milliseconds */
mstime_t mstime(void);

void initQueue(struct Queue *queue);

/* Generate the next item ID given the previous one. If the current
 * milliseconds Unix time is greater than the previous one, just use this
 * as time part and start with sequence part of zero. Otherwise we use the
 * previous time (and never go backward) and increment the sequence. */
void setNextMsgID(struct MsgID *last_id, struct MsgID *new_id);

