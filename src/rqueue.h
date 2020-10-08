#include <stdint.h>
#include <sys/time.h>
#define REDISMODULE_EXPERIMENTAL_API
#include "../redismodule.h"

#define RQUEUE_ENCODING_VERSION 0
#define MSG_ID_FORMAT "%lu-%lu"

typedef long long mstime_t; /* millisecond time type. */

/**
 * Block of contiguous msg_t structs
 */
typedef struct msg_block_t {
    void *ptr;        //Pointer to the first msg in the block
    size_t count;     //Total count of messages inside the block
    size_t acked;     //total messages in the block that has been acknowledged.
    //size_t mem_usage; //Total memory usage of all nodes in this block
} msg_block_t;

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
    msg_block_t *block; // msg block to which the msg belongs
} msg_t;

typedef struct queue_t {
    void *first; /* First to be served */
    void *last;
    size_t len; /* Number of elements added. */
} queue_t;

/**
 * Reliable Queue Object 
 */
typedef struct rqueue_t {
    RedisModuleString *name; // Redis key for this RQ
    msgid_t last_id;     // Zero if there are yet no items
    queue_t undelivered; // never-delivered queue
    queue_t delivered;   // Queue of messages that has being delivered at-least-one 
    size_t memory_used;
} rqueue_t;

/**
 * POP Arguments
 */
typedef struct rq_pop_t {
    uint64_t count;
    int64_t block;
    uint key_count;
    RedisModuleString **keys;
} rq_pop_t;

long long mstime(void);

/* Return the UNIX time in milliseconds */
mstime_t mstime(void);

void initQueue(queue_t *queue);

// parses a pop command args
int rq_parse_pop_args(
    RedisModuleCtx *ctx,
    RedisModuleString **argv,
    int argc,
    rq_pop_t *pop
);

/** Creates and initializes a new RELIABLEQ object, and returns a pointer to it. */
rqueue_t *rqueueCreate(const RedisModuleString *name);

/* Generate the next item ID given the previous one. If the current
 * milliseconds Unix time is greater than the previous one, just use this
 * as time part and start with sequence part of zero. Otherwise we use the
 * previous time (and never go backward) and increment the sequence. */
void setNextMsgID(msgid_t *last_id, msgid_t *new_id);

/**
 * Pops up to "count" messages from the reliable queue at "rqueue", and replies
 * to the Redis client
 */
long long popAndReply(
	RedisModuleCtx *ctx,
	rqueue_t *rqueue,
	long long *count
);

/* Blocking commands callbacks */
//void rq_unblock_clients(RedisModuleCtx *ctx, rqueue_t *rqueue, int count);
//int bpop_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int bpop_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void bpop_freeData(RedisModuleCtx *ctx, void *privdata);
void bpop_disconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc);

size_t rq_memory_usage(const void *value);

/* RDB and AOF handlers */
void RQueueRdbSave(RedisModuleIO *rdb, void *value);
void *RQueueRdbLoad(RedisModuleIO *rdb, int encver);
void RQueueReleaseObject(void *value);
