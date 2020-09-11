#include <stdint.h>
#include <sys/time.h>
#include "../redismodule.h"

#define MSG_ID_FORMAT "%lu-%lu"

typedef long long mstime_t; /* millisecond time type. */

/* Queue item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
typedef struct msgid_t {
    uint64_t ms;        /* Unix time in milliseconds. */
    uint64_t seq;       /* Sequence number. */
} msgid_t;

typedef struct msg_t {
    msgid_t id;
    RedisModuleString *value;
    struct msg_t *next;
    uint deliveries; /* how many times the msg has being delivered*/
    mstime_t lastDelivery; /* Last time the msg was delivered */
} msg_t;

typedef struct queue_t {
    msg_t *first; /* First to be served */
    msg_t *last;
    size_t len; /* Number of elements added. */
} queue_t;

/**
 * Reliable Queue Object 
 */
typedef struct rqueue_t {
    msgid_t last_id;       /* Zero if there are yet no items. */
    queue_t undelivered;
    queue_t delivered; /* Queue of messages that has being delivered at-least-one */
} rqueue_t;

typedef struct bpopclient_t {
    
} bpopclient_t;

long long mstime(void);

/* Return the UNIX time in milliseconds */
mstime_t mstime(void);

void initQueue(queue_t *queue);

/* Generate the next item ID given the previous one. If the current
 * milliseconds Unix time is greater than the previous one, just use this
 * as time part and start with sequence part of zero. Otherwise we use the
 * previous time (and never go backward) and increment the sequence. */
void setNextMsgID(msgid_t *last_id, msgid_t *new_id);

/**
 * Pops up to "count" messages from the reliable queue at "rqueue", and replies
 * to the Redis client
 */
int popAndReply(rqueue_t *rqueue, long long count, RedisModuleCtx *ctx);

/* Blocking commands callbacks */
int bpop_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int bpop_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void bpop_freeData(RedisModuleCtx *ctx, void *privdata);
void bpop_disconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc);
