#include <sys/time.h>
#include "./rqueue.h"

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the current UNIX time in milliseconds */
mstime_t mstime(void) {
	struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;

    return ust/1000;
}

void initQueue(queue_t *queue){
	queue->len = 0;
	queue->first = NULL;
	queue->last = NULL;
}

/* Generate the next item ID given the previous one. If the current
 * milliseconds Unix time is greater than the previous one, just use this
 * as time part and start with sequence part of zero. Otherwise we use the
 * previous time (and never go backward) and increment the sequence. */
void setNextMsgID(msgid_t *last_id, msgid_t *new_id) {
    uint64_t ms = mstime();
    if (ms > last_id->ms) {
        new_id->ms = ms;
        new_id->seq = 1;
    } else {
        *new_id = *last_id;
		if (new_id->seq == UINT64_MAX) {
			if (new_id->ms == UINT64_MAX) {
				/* Special case where 'new_id' is the last possible streamID... */
				new_id->ms = new_id->seq = 0;
			} else {
				new_id->ms++;
				new_id->seq = 0;
			}
		} else {
			new_id->seq++;
		}
    }
}

/**
 * Creates and initializes a fresh new RQUEUE object
 * @return rqueue_t * Pointer to the created object
 */
rqueue_t *createRQueueObject(){
	rqueue_t *rqueue = RedisModule_Alloc(sizeof(*rqueue));
	rqueue->last_id.ms = 0;
    rqueue->last_id.seq = 0;
	initQueue(&rqueue->undelivered);
	initQueue(&rqueue->delivered);
	
	return rqueue;
}

int popAndReply(rqueue_t *rqueue, long long count, RedisModuleCtx *ctx)
{
	if(rqueue->undelivered.first == NULL){
		return RedisModule_ReplyWithNull(ctx);
	}

	msg_t *topop = rqueue->undelivered.first,
		*next;

    long long actually_poped = 0;

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

	while (count > 0 && topop != NULL)
	{
		next = topop->next;
		topop->lastDelivery = mstime();
		topop->deliveries += 1;

		// Update "undelivered" queue
		rqueue->undelivered.first = topop->next;
		rqueue->undelivered.len -= 1;

		// Update "delivered" queue
		if(rqueue->delivered.first == NULL || rqueue->delivered.last == NULL){
			rqueue->delivered.first = rqueue->delivered.last = topop;
		} else {
			rqueue->delivered.last->next = topop;
			rqueue->delivered.last = topop;
		}
		rqueue->delivered.len += 1;
		topop->next = NULL;

		// Finally: reply with a 2-element-array with: MsgID and the payload
		RedisModule_ReplyWithArray(ctx, 2);
		RedisModule_ReplyWithString(
			ctx,
			RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, topop->id.ms, topop->id.seq)
		);
		RedisModule_ReplyWithString(ctx, topop->value);

		// Move-on to the next element to pop
		count -= 1;
		actually_poped += 1;
		topop = next;
	}

	RedisModule_ReplySetArrayLength(ctx, actually_poped);

	return REDISMODULE_OK;
}

/* ============= RDB and AOF callbacks ==================*/

void RQueueRdbSave(RedisModuleIO *rdb, void *value) {
    rqueue_t *rqueue = value;
    msg_t *node; // = r;

    RedisModule_SaveUnsigned(rdb, rqueue->undelivered.len);
	RedisModule_SaveUnsigned(rdb, rqueue->delivered.len);
	
	// First: persist undelivered list
	node = rqueue->undelivered.first;
    while(node) {
        RedisModule_SaveUnsigned(rdb,node->id.ms);
        RedisModule_SaveUnsigned(rdb,node->id.seq);
		RedisModule_SaveString(rdb, node->value);
        node = node->next;
    }

	// Second: persist delivered elements
	node = rqueue->delivered.first;
    while(node) {
        RedisModule_SaveUnsigned(rdb,node->id.ms);
        RedisModule_SaveUnsigned(rdb,node->id.seq);
		RedisModule_SaveString(rdb, node->value);
		RedisModule_SaveUnsigned(rdb,node->deliveries);
		RedisModule_SaveUnsigned(rdb,node->lastDelivery);
        node = node->next;
    }
}

void *RQueueRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != RQUEUE_ENCODING_VERSION) {
        RedisModule_Log(NULL, "warning", "Can't load data with version %d. Current supported version: %d",
			encver, RQUEUE_ENCODING_VERSION);
        return NULL;
    }

	rqueue_t *rqueue = createRQueueObject();

    rqueue->undelivered.len = RedisModule_LoadUnsigned(rdb);
    rqueue->delivered.len  = RedisModule_LoadUnsigned(rdb);
	uint64_t total_messages = rqueue->undelivered.len + rqueue->delivered.len;

	if(total_messages == 0){
		return rqueue;
	}

	msg_t *msg = RedisModule_Alloc(sizeof(*msg) * total_messages);

	// load "undelivered" messages
	if(rqueue->undelivered.len > 0)
	{
		rqueue->undelivered.first = &msg[0];

		for(
			uint64_t i = 0, j = 1;
			i < rqueue->undelivered.len;
			i++, j++
		){
			msg[i].id.ms = RedisModule_LoadUnsigned(rdb);
			msg[i].id.seq = RedisModule_LoadUnsigned(rdb);
			msg[i].value = RedisModule_LoadString(rdb);
			msg[i].deliveries = 0;
			msg[i].lastDelivery = 0;
			msg[i].next = (
				j < rqueue->undelivered.len ?
				&msg[j] :
				NULL
			);
		}

		rqueue->undelivered.last = &msg[rqueue->undelivered.len - 1];
	}
	

	// load "delivered" messages
	if(rqueue->delivered.len > 0)
	{
		rqueue->delivered.first = &msg[rqueue->delivered.len];

		for(
			uint64_t i = 0, j = 1, k = rqueue->delivered.len;
			i < rqueue->delivered.len;
			i++, j++, k++
		){
			msg[k].id.ms = RedisModule_LoadUnsigned(rdb);
			msg[k].id.seq = RedisModule_LoadUnsigned(rdb);
			msg[k].value = RedisModule_LoadString(rdb);
			msg[k].deliveries = RedisModule_LoadUnsigned(rdb);
			msg[k].lastDelivery = RedisModule_LoadUnsigned(rdb);
			msg[k].next = (
				j < rqueue->delivered.len ?
				&msg[k + 1] :
				NULL
			);
		}

		rqueue->delivered.last = &msg[rqueue->delivered.len - 1];
	}
	
	return rqueue;
}


void RQueueReleaseObject(void *value) {
	rqueue_t *rqueue = value;
	msg_t *cur, *next;

	 // Free all undelivered message
    cur = rqueue->undelivered.first;
    while(cur) {
		next = cur->next;
		RedisModule_FreeString(NULL, cur->value);
        RedisModule_Free(cur);
        cur = next;
    }

	 // Free all delivered message
    cur = rqueue->delivered.first;
    while(cur) {
        next = cur->next;
        RedisModule_Free(cur);
        cur = next;
    }

    RedisModule_Free(value);
}

/* Reply callback for blocking command MQ.BPOP */
/*int bpop_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    int *myint = RedisModule_GetBlockedClientPrivateData(ctx);
    return RedisModule_ReplyWithLongLong(ctx,*myint);
}*/

/* Timeout callback for blocking command MQ.BPOP */
/*int bpop_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    return RedisModule_ReplyWithSimpleString(ctx,"Request timedout");
}*/

/* Private data freeing callback for MQ.BPOP command. */
/*void bpop_freeData(RedisModuleCtx *ctx, void *privdata) {
    REDISMODULE_NOT_USED(ctx);
    RedisModule_Free(privdata);
}*/

/* An example blocked client disconnection callback.
 *
 * Note that in the case of the HELLO.BLOCK command, the blocked client is now
 * owned by the thread calling sleep(). In this specific case, there is not
 * much we can do, however normally we could instead implement a way to
 * signal the thread that the client disconnected, and sleep the specified
 * amount of seconds with a while loop calling sleep(1), so that once we
 * detect the client disconnection, we can terminate the thread ASAP. */
/*void bpop_disconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    RedisModule_Log(ctx,"warning","Blocked client %p disconnected!",
        (void*)bc);

    // Here you should cleanup your state / threads, and if possible
    // call RedisModule_UnblockClient(), or notify the thread that will
    // call the function ASAP.
} */