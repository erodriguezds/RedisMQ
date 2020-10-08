#include <sys/time.h>
#define REDISMODULE_EXPERIMENTAL_API
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"
#include "./module.h"
#include "./rqueue.h"
#include "./error.h"

static RedisModuleType *RELIABLEQ_TYPE;

/**
 * Return info on a given queue
 */
int infoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
	int type = RedisModule_KeyType(key);
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, ERRORMSG_EMPTYKEY);
	}

	if(RedisModule_ModuleTypeGetType(key) != RELIABLEQ_TYPE){
		return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	rqueue_t *rqueue = RedisModule_ModuleTypeGetValue(key);

	RedisModule_ReplyWithArray(ctx,4);

	RedisModule_ReplyWithCString(ctx, "undelivered");
	RedisModule_ReplyWithLongLong(ctx, rqueue->undelivered.len);

	RedisModule_ReplyWithCString(ctx, "delivered");
	RedisModule_ReplyWithLongLong(ctx, rqueue->delivered.len);

	return REDISMODULE_OK;
}

/* Reply callback for blocked RQ.POP */
int bpop_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	rq_pop_t popargs;
	long long count;
	RedisModuleString *key = RedisModule_GetBlockedClientReadyKey(ctx);
	RedisModuleKey *keyobj = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
	
	if(RedisModule_KeyType(keyobj) == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(keyobj);
		return REDISMODULE_ERR;
	}

	if(RedisModule_ModuleTypeGetType(keyobj) != RELIABLEQ_TYPE){
		RedisModule_CloseKey(keyobj);
		return REDISMODULE_ERR;
	}

	rqueue_t *rqueue = RedisModule_ModuleTypeGetValue(keyobj);

	if(rqueue->undelivered.len == 0){
		RedisModule_CloseKey(keyobj);
		return REDISMODULE_ERR;
	}

	rq_parse_pop_args(ctx, argv, argc, &popargs);
	count = popargs.count;

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	long long poped = popAndReply(ctx, rqueue, &count);
	RedisModule_ReplySetArrayLength(ctx, poped);
	
	return REDISMODULE_OK;
}

/**
 * rq.push <key> <msg1> [ <msg2> [...]]
 * Pushes 1 or more items into key
 */
int pushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

	rqueue_t *rqueue;
	msg_t *newmsg;
	msg_block_t *block = NULL;
	int count = argc - 2; // count of new messages being pushed
	size_t strlen; // dummy value for storing the length of RedisModuleString objects

	if (argc < 3) return RedisModule_WrongArity(ctx);

   RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
   int type = RedisModule_KeyType(key);
   
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		// Key doesn't exist. Create...
		rqueue = rqueueCreate(argv[1]);
		RedisModule_ModuleTypeSetValue(key, RELIABLEQ_TYPE, rqueue);
	} else if(RedisModule_ModuleTypeGetType(key) == RELIABLEQ_TYPE){
		// key exists. Get and update
		rqueue = RedisModule_ModuleTypeGetValue(key);
	} else {
		// Key exists, but it's not an RQueueObject!!!
		return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	// allocate the memory
	if(count > 1){
		block = RedisModule_Alloc(sizeof(*block));
		block->ptr = RedisModule_Calloc(count, sizeof(*newmsg));
		block->count = count;
		block->acked = 0;
		//block->mem_usage = (sizeof(*newmsg) * count);
		newmsg = block->ptr;
		rqueue->memory_used += sizeof(*block) + (sizeof(*newmsg) * count);
	} else {
		newmsg = RedisModule_Alloc(sizeof(*newmsg));
		rqueue->memory_used += sizeof(*newmsg);
	}

	RedisModule_ReplyWithArray(ctx, count);

	// Init the new nodes
	for(int i = 0, j = 1; i < count; i++, j++){
		//TODO: refactor this
		if(i == 0){
			setNextMsgID(&rqueue->last_id, &newmsg[i].id);
		} else {
			setNextMsgID(&newmsg[i - 1].id, &newmsg[i].id);
		}
		newmsg[i].lastDelivery = 0;
		newmsg[i].deliveries = 0;
		newmsg[i].next = (
			j < count ?
			&newmsg[j] :
			NULL
		);
		newmsg[i].value = RedisModule_HoldString(NULL, argv[2 + i]);
		newmsg[i].block = block;
		
		//memory usage stats
		RedisModule_StringPtrLen(newmsg[i].value, &strlen);
		rqueue->memory_used += strlen;

		RedisModule_ReplyWithString(
			ctx,
			RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, newmsg[i].id.ms, newmsg[i].id.seq)
		);
	}

	//Update first node
	if(rqueue->undelivered.first == NULL){
		rqueue->undelivered.first = &newmsg[0];
	} else {
		((msg_t*) rqueue->undelivered.last)->next = &newmsg[0];
	}

	// Update last node
	rqueue->undelivered.last = &newmsg[count - 1];

	// Update count
	rqueue->undelivered.len += count;

	// Update last_id
	rqueue->last_id = newmsg[count - 1].id;

	// Unblock clients
	RedisModule_SignalKeyAsReady(ctx, argv[1]);
	//RedisModule_CloseKey(argv[1]);

	return REDISMODULE_OK;
}

/**
 * RQ.INSPECT <key> [ PENDING ] <start> [ <count> ]
 * 
 * Inspects <count> elements at the "undelivered" queue, starting at <start>.
 * If PENDING is provided after the <key> to inspect, then the elements at the
 * "delivered" queue will be inspected instead.
 **/
int inspectCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

	if (argc < 4){
		return RedisModule_WrongArity(ctx);
	}

	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
   
	if(RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx,ERRORMSG_EMPTYKEY);
	}

	if(RedisModule_ModuleTypeGetType(key) != RELIABLEQ_TYPE){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	long long start = 0, count, pos = 0, outputed = 0;
	int pending = RMUtil_StringEqualsCaseC(argv[2], "PENDING");

	if(RMUtil_ParseArgs(argv, argc, (pending ? 3 : 2), "ll", &start, &count) != REDISMODULE_OK){
		return RedisModule_ReplyWithError(ctx, MQ_ERROR_INSPECT_USAGE);
	}

	if(count <= 0){
		return RedisModule_ReplyWithError(ctx, MQ_ERROR_INSPECT_USAGE);
	}

	rqueue_t *rqueue = RedisModule_ModuleTypeGetValue(key);
	queue_t *queue = (
		pending ?
		&rqueue->delivered :
		&rqueue->undelivered
	);
	msg_t *cur = NULL;

	if(start < 0){
		start += queue->len;
	}

	// Set the starting node
	if(start < 0 || start > (queue->len - 1)){
		return RedisModule_ReplyWithArray(ctx, 0);
	}

	// Walk the list starting with the fisrt node
	cur = queue->first;
	while (pos < start && cur->next != NULL)
	{
		cur = cur->next;
		pos += 1;
	}

	// Now, walk the list from the current node, onwards
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

	if(pending){
		mstime_t now = mstime();
		while (cur && outputed < count)
		{
			RedisModule_ReplyWithArray(ctx,5);
			RedisModule_ReplyWithString(
				ctx,
				RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, cur->id.ms, cur->id.seq)
			);
			RedisModule_ReplyWithString(ctx, cur->value);
			RedisModule_ReplyWithLongLong(ctx, cur->lastDelivery);
			RedisModule_ReplyWithLongLong(ctx, now - cur->lastDelivery);
			RedisModule_ReplyWithLongLong(ctx, cur->deliveries);
			outputed += 1;
			cur = cur->next;
		}
	} else {
		while(cur && outputed < count)
		{
			RedisModule_ReplyWithArray(ctx,2);
			RedisModule_ReplyWithString(
				ctx,
				RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, cur->id.ms, cur->id.seq)
			);
			//RedisModule_ReplyWithLongLong(ctx, cur->lastDelivery);
			//RedisModule_ReplyWithLongLong(ctx, cur->deliveries);
			RedisModule_ReplyWithString(ctx, cur->value);
			outputed += 1;
			cur = cur->next;
		}
	}
	
	RedisModule_ReplySetArrayLength(ctx, outputed);
	
	return REDISMODULE_OK;
}

/**
 * Usage: RQ.POP <count> [ BLOCK <ms> ] <key>
 * 
 * Pops <count> elements from the reliable queue at <key>.
 * The poped elements are placed into the internal "delivered" list, for
 * later acknoledgment.
 */
int popCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc < 2) return RedisModule_WrongArity(ctx);

	rq_pop_t popargs;

	if(rq_parse_pop_args(ctx, argv, argc, &popargs)){
		return RedisModule_ReplyWithError(ctx, MQ_ERROR_POP_USAGE);
	}

	long long count = popargs.count;
	long long total_poped = 0;
	rqueue_t *rqueue = NULL;
	//uint valid_keys = 0;

	//First, check all keys
	for(
		int k = 0;
		k < popargs.key_count && count > 0;
		k++
	){
		RedisModuleString *qname = popargs.keys[k];
		RedisModuleKey *key = RedisModule_OpenKey(ctx, qname, REDISMODULE_READ|REDISMODULE_WRITE);
		int type = RedisModule_KeyType(key);
	
		if(type == REDISMODULE_KEYTYPE_EMPTY){
			//valid_keys++; // It's valid for BLOCKING pop
			continue;
			//return RedisModule_ReplyWithNull(ctx);
		}
		
		if(RedisModule_ModuleTypeGetType(key) != RELIABLEQ_TYPE){
			if(total_poped == 0){
				return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
			}

			continue;
		}

		//valid_keys++;

		rqueue = RedisModule_ModuleTypeGetValue(key);

		if(rqueue->undelivered.len == 0 || rqueue->undelivered.first == NULL){
			continue;
		}

		if(total_poped == 0){
			RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
		}

		total_poped += popAndReply(ctx, rqueue, &count);
	}

	if(total_poped > 0){
		RedisModule_ReplySetArrayLength(ctx, total_poped);
		return REDISMODULE_OK;
	}

	if(popargs.block == 0){
		return RedisModule_ReplyWithArray(ctx, 0);
	}
	
	//BLOCKING behaviour. Add rq_blocked_client_t to every key
	RedisModule_BlockClientOnKeys(
		ctx,
		bpop_reply,
		bpop_timeout,
		bpop_freeData,
		(popargs.block < 0 ? 0 : popargs.block),
		popargs.keys,
		popargs.key_count,
		NULL //privdata
	);

	return REDISMODULE_OK;
}

/**
 * ACK <queue> <msgid1> [ <msgid2> [ ... ] ]
 * 
 * Acknowledges the successful processing of 1 or more messages, removing them
 * from the internal "delivered" queue.
 * 
 * Returns: ARRAY of messages ID's found and removed
 */
int ackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc < 3) return RedisModule_WrongArity(ctx);

	// Retrieve the key content
	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
   
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithNull(ctx);
	}
	
	if(RedisModule_ModuleTypeGetType(key) != RELIABLEQ_TYPE){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	rqueue_t *rqueue = RedisModule_ModuleTypeGetValue(key);

	if(rqueue->delivered.first == NULL){
		return RedisModule_ReplyWithArray(ctx,0);
	}

	msg_t *cur, *next, *prev;
	msg_block_t *block;
	msgid_t id;
	char *idptr;
	size_t idlen;
	char idbuf[128];
	long removed = 0;
	size_t strlen;
	
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

	for(int i = 2; i < argc; i++){

		//parse the ID
		idptr = RedisModule_StringPtrLen(argv[i], &idlen);
		memcpy(idbuf, idptr, idlen);
		idbuf[idlen < 128 ? idlen : 127] = (char) 0;
		sscanf(idbuf, MSG_ID_FORMAT, &id.ms, &id.seq);

		cur = rqueue->delivered.first;
		prev = NULL;
		while(cur){
			next = cur->next;
			if(cur->id.ms == id.ms && cur->id.seq == id.seq){

				//Link previous node to next node
				if(prev != NULL){
					prev->next = cur->next;
				}

				if(cur == rqueue->delivered.first){
					rqueue->delivered.first = cur->next;
				}

				if(cur == rqueue->delivered.last){
					rqueue->delivered.last = prev;
				}
				
				//Free string
				RedisModule_StringPtrLen(cur->value, &strlen);
				RedisModule_FreeString(NULL, cur->value);
				cur->value = NULL;
				rqueue->memory_used -= strlen;

				if(cur->block){
					block = cur->block;
					block->acked += 1;
					//block->mem_usage -= strlen;
					if(block->acked >= block->count){
						size_t mem_to_free = (sizeof(*cur) * block->count) + sizeof(*block);

						//Free entire block
						RedisModule_Free(block->ptr);
						block->ptr = cur = NULL;

						//Free block struct
						RedisModule_Free(block);
						block = NULL;

						// update rqobj memory usage before freeing
						rqueue->memory_used -= mem_to_free;
					}
				} else {
					RedisModule_Free(cur);
					cur = NULL;
				}
				removed++;
				rqueue->delivered.len -= 1;
				RedisModule_ReplyWithString(ctx, argv[i]);
				break;
			} else {
				prev = cur;
			}
			cur = next;
		};
	}

	RedisModule_ReplySetArrayLength(ctx, removed);

	return REDISMODULE_OK;
}

/**
 * RECOVER <key> <count> <elapsed>
 * 
 * Recovers <count> elements from the <key> delivered queue that has been delivered
 * and not acknoledged for <elapsed> milliseconds or more.
 * 
 * Returns: Array of messages recovered
 */
int recoverCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc < 3) return RedisModule_WrongArity(ctx);

	// Retrieve the key content
	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
   
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithNull(ctx);
	}
	
	if(RedisModule_ModuleTypeGetType(key) != RELIABLEQ_TYPE){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	rqueue_t *rqueue = RedisModule_ModuleTypeGetValue(key);

	if(rqueue->delivered.first == NULL){
		return RedisModule_ReplyWithArray(ctx,0);
	}

	long long count, elapsed, recovered = 0;
	int pending = RMUtil_StringEqualsCaseC(argv[2], "PENDING");

	if(RMUtil_ParseArgs(argv, argc, (pending ? 3 : 2), "ll", &count, &elapsed) != REDISMODULE_OK){
		return RedisModule_ReplyWithError(ctx, MQ_ERROR_RECOVER_USAGE);
	}

	msg_t *cur = rqueue->delivered.first, *next; //, *prev;
	mstime_t now = mstime();

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

	while (
		cur &&
		(now - cur->lastDelivery) >= elapsed &&
		recovered < count
	)
	{
		next = cur->next;

		//Reply
		RedisModule_ReplyWithArray(ctx, 2);
		RedisModule_ReplyWithString(
			ctx,
			RedisModule_CreateStringPrintf(ctx, MSG_ID_FORMAT, cur->id.ms, cur->id.seq)
		);
		RedisModule_ReplyWithString(ctx, cur->value);

		//Update delivery info
		cur->lastDelivery = now;
		cur->deliveries += 1;
		recovered += 1;

		//Move current node to the end of the delivered queue
		if(cur->next){
			rqueue->delivered.first = cur->next;
			((msg_t *)rqueue->delivered.last)->next = cur;
			rqueue->delivered.last = cur;
			cur->next = NULL;
		}

		cur = next;
	}
	
	RedisModule_ReplySetArrayLength(ctx, recovered);

	return REDISMODULE_OK;
}

void QueueAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	//TODO
	return;
}


/* EXAMPLE.PARSE [SUM <x> <y>] | [PROD <x> <y>]
*  Demonstrates the automatic arg parsing utility.
*  If the command receives "SUM <x> <y>" it returns their sum
*  If it receives "PROD <x> <y>" it returns their product
*/
int ParseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

  // we must have at least 4 args
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  // init auto memory for created strings
  
  long long x, y;

  // If we got SUM - return the sum of 2 consecutive arguments
  if (RMUtil_ParseArgsAfter("SUM", argv, argc, "ll", &x, &y) ==
      REDISMODULE_OK) {
    RedisModule_ReplyWithLongLong(ctx, x + y);
    return REDISMODULE_OK;
  }

  // If we got PROD - return the product of 2 consecutive arguments
  if (RMUtil_ParseArgsAfter("PROD", argv, argc, "ll", &x, &y) ==
      REDISMODULE_OK) {
    RedisModule_ReplyWithLongLong(ctx, x * y);
    return REDISMODULE_OK;
  }

  // something is fishy...
  RedisModule_ReplyWithError(ctx, "Invalid arguments");

  return REDISMODULE_ERR;
}





// Test the the PARSE command
int testParse(RedisModuleCtx *ctx) {

  RedisModuleCallReply *r =
      RedisModule_Call(ctx, "example.parse", "ccc", "SUM", "5", "2");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "7");

  r = RedisModule_Call(ctx, "example.parse", "ccc", "PROD", "5", "2");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "10");
  return 0;
}

// test the HGETSET command
int testHgetSet(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r =
      RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "baz");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  r = RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "bag");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING);
  RMUtil_AssertReplyEquals(r, "baz");
  r = RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "bang");
  RMUtil_AssertReplyEquals(r, "bag");
  return 0;
}

// Unit test entry point for the module
int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  RMUtil_Test(testParse);
  RMUtil_Test(testHgetSet);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

	// Register the module itself
	if (RedisModule_Init(ctx, "mq", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

	// Register the ReliableQueue Type
	RedisModuleTypeMethods tm = {
		.version = REDISMODULE_TYPE_METHOD_VERSION,
    	.rdb_load = RQueueRdbLoad,
    	.rdb_save = RQueueRdbSave,
    	.aof_rewrite = QueueAofRewrite,
		.mem_usage = rq_memory_usage,
    	.free = RQueueReleaseObject
	};

   RELIABLEQ_TYPE = RedisModule_CreateDataType(
		ctx,
		"RELIABLEQ",
		RQUEUE_ENCODING_VERSION,
		&tm
	);

   if (RELIABLEQ_TYPE == NULL) return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"rq.push", pushCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"rq.pop", popCommand,"write",2,2,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"rq.ack", ackCommand,"write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"rq.recover", recoverCommand,"write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	// register xq.info - the default registration syntax
	if (RedisModule_CreateCommand(ctx, "rq.info", infoCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

	// register xq.parse - the default registration syntax
	if (RedisModule_CreateCommand(ctx, "rq.inspect", inspectCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

	// register the unit test
	RMUtil_RegisterWriteCmd(ctx, "rq.test", TestModule);
	

	return REDISMODULE_OK;
}
