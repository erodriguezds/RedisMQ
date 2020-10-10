#include <sys/time.h>
#include "./rqueue.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"

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

int rq_parse_pop_args(
    RedisModuleCtx *ctx,
    RedisModuleString **argv,
    int argc,
    rq_pop_t *pop
){
	long long temp = 0;
	int k = 1;// pointer to first possible key
	int left = argc - 1; //arguments left

	//Init pop arguments
	pop->count = 1;
	pop->block = 0;
	pop->key_count = 0;

	// Parse COUNT, if provided
	if(
		left >= 3 &&
		RMUtil_StringEqualsCaseC(argv[k], "COUNT") &&
		RMUtil_ParseArgs(argv, argc, 2, "l", &temp) == REDISMODULE_OK
	){
		if(temp < 0){
			return 1;
		}

		pop->count = temp;
		k += 2;
		left -= 2;
	}

	if(
		left >= 3 &&
		RMUtil_StringEqualsCaseC(argv[k], "BLOCK") &&
		RMUtil_ParseArgs(argv, argc, k + 1, "l", &temp) == REDISMODULE_OK
	){
		pop->block = temp;
		k += 2;
		left -= 2;
	}

	pop->key_count = argc - k;
	pop->keys = &argv[k];

	return REDISMODULE_OK;
}

/**
 * Creates and initializes a fresh new RQUEUE object
 * @return rqueue_t * Pointer to the created object
 */
rqueue_t *rqueueCreate(const RedisModuleString *name){
	rqueue_t *rqueue = RedisModule_Alloc(sizeof(*rqueue));
	rqueue->name = RedisModule_CreateStringFromString(NULL, name);
	rqueue->last_id.ms = 0;
	rqueue->last_id.seq = 0;
	initQueue(&rqueue->undelivered);
	initQueue(&rqueue->delivered);
	rqueue->memory_used = sizeof(*rqueue);
	
	return rqueue;
}

/**
 * @return int The items actually poped
 */
long long popAndReply(
	RedisModuleCtx *ctx,
	rqueue_t *rqueue,
	long long *count
)
{
	if(*count <= 0 || rqueue->undelivered.len == 0 || rqueue->undelivered.first == NULL){
		return 0;
	}

	msg_t *topop = rqueue->undelivered.first,
		*next;

    long long actually_poped = 0;

	while (*count > 0 && topop != NULL)
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
			((msg_t *)rqueue->delivered.last)->next = topop;
			rqueue->delivered.last = topop;
		}
		rqueue->delivered.len += 1;
		topop->next = NULL;

		// Finally: reply with a 2-element-array with: MsgID and the payload
		RedisModule_ReplyWithArray(ctx, 3);
		RedisModule_ReplyWithString(ctx, rqueue->name);
		RedisModule_ReplyWithString(
			ctx,
			RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, topop->id.ms, topop->id.seq)
		);
		RedisModule_ReplyWithString(ctx, topop->value);

		// Move-on to the next element to pop
		*count = *count - 1;
		actually_poped += 1;
		topop = next;
	}

	return actually_poped;
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

void *rq_rdb_load(RedisModuleIO *rdb, int encver) {
    if (encver != RQUEUE_ENCODING_VERSION) {
        RedisModule_Log(NULL, "warning", "Can't load data with version %d. Current supported version: %d",
			encver, RQUEUE_ENCODING_VERSION);
        return NULL;
    }

	rqueue_t *rqueue = rqueueCreate(RedisModule_GetKeyNameFromIO(rdb));
    rqueue->undelivered.len = RedisModule_LoadUnsigned(rdb);
    rqueue->delivered.len  = RedisModule_LoadUnsigned(rdb);
	uint64_t total_messages = rqueue->undelivered.len + rqueue->delivered.len;

	if(total_messages == 0){
		return rqueue;
	}

	msg_t *msg = NULL, *prev = NULL;
	size_t strlen;

	for(
		uint64_t i = 0, left = total_messages;
		left > 0;
		//do-nothing
	){
		msg_block_t *block = RedisModule_Alloc(sizeof(*block));
		size_t numelem = ( left > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : left );
		block->ptr = RedisModule_Calloc(numelem, sizeof(*msg));
		block->count = numelem;
		block->freed = 0;
		rqueue->memory_used += sizeof(*block) + (sizeof(*msg) * numelem);
		
		//Init the messages
		msg = block->ptr;
		for(int j = 0; j < block->count; j++, i++, left--){
			msg[j].block = block;
			msg[j].id.ms = RedisModule_LoadUnsigned(rdb);
			msg[j].id.seq = RedisModule_LoadUnsigned(rdb);
			msg[j].value = RedisModule_LoadString(rdb);
			RedisModule_StringPtrLen(msg[j].value, &strlen);
			rqueue->memory_used += strlen;
			if(i < rqueue->undelivered.len){
				msg[j].deliveries = 0;
				msg[j].lastDelivery = 0;
				msg[j].next = NULL; //will be initialized in next cycle
				if(prev){
					prev->next = &msg[j];
				}
			} else {
				msg[j].deliveries = RedisModule_LoadUnsigned(rdb);
				msg[j].lastDelivery = RedisModule_LoadUnsigned(rdb);
				msg[j].next = NULL; //will be initialized in next cycle
				if(prev && prev->deliveries > 0){
					prev->next = &msg[j];
				}
			}
			prev = &msg[j];
		}
	}

	// load "undelivered" messages
	/*if(rqueue->undelivered.len > 0)
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
	}*/
	
	return rqueue;
}

/* The goal of this function is to return the amount of memory used by
 * the HelloType value. */
size_t rq_memory_usage(const void *value) {
    const rqueue_t *rqobj = value;

    return rqobj->memory_used;
}

// Frees all the memory used by the messages in a queue
void free_mq(msg_t *first){
	msg_t *cur = NULL, *next = NULL;
	msg_block_t *block = NULL;

	cur = first;
	while(cur) {
		next = cur->next;
		RedisModule_FreeString(NULL, cur->value);
		cur->value = NULL;
		if(cur->block){
			block = cur->block;
			block->freed += 1;
			if(block->freed >= block->count){
				RedisModule_Free(block->ptr);
				block->ptr = cur = NULL;
				RedisModule_Free(block);
				block = NULL;
			}
		} else {
        	RedisModule_Free(cur);
		}
        cur = next;
    }
}

void rq_free(void *value) {
	rqueue_t *rqueue = value;

	 // Free all undelivered message
	free_mq(rqueue->undelivered.first);
	free_mq(rqueue->delivered.first);

	// Free name string
	RedisModule_FreeString(NULL, rqueue->name);

    RedisModule_Free(value);
}

/* Timeout callback for blocked RQ.POP */
int bpop_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	return RedisModule_ReplyWithNull(ctx);
}

/* Private data freeing callback for RQ.BPOP command. */
void bpop_freeData(RedisModuleCtx *ctx, void *privdata) {
    REDISMODULE_NOT_USED(ctx);
    //RedisModule_Free(privdata);
	RedisModule_Log(ctx,"debug","Freeing bpop privdata at %p", privdata);
}
