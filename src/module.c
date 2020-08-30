#include <sys/time.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"
#include "./module.h"

static RedisModuleType *RQueueRedisType;
#define RQUEUE_ENCODING_VERSION 0

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
	struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;

    return ust/1000;
}

/**
 *  Compare a Redis string value against a native string.
 *  If 0 is returned, the strings are equal.
*/
int redis_stricmp(RedisModuleString *redis_str, const char *token){
	size_t l;
	size_t toklen = strlen(token);
    const char *redis_raw_str = RedisModule_StringPtrLen(redis_str, &l);
	return strncasecmp(redis_raw_str, token, toklen);
}

//TODO
void QueueRDBSave(RedisModuleIO *io, void *ptr) {
}

//TODO: Implement RDBLoad
void *QueueRDBLoad(RedisModuleIO *io, int encver) {
	return NULL;
}

void QueueAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
   struct RQueueObject *rqueue = value;
   struct Msg *node;
	 
	node = rqueue->undelivered.first;
   while(node) {
   	RedisModule_EmitAOF(aof,"mq.push","sl",key,node->value);
   	node = node->next;
   }
}

void QueueReleaseObject(void *value) {
	struct RQueueObject *rqueue = value;
	struct Msg *cur, *next;

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


/* EXAMPLE.PARSE [SUM <x> <y>] | [PROD <x> <y>]
*  Demonstrates the automatic arg parsing utility.
*  If the command receives "SUM <x> <y>" it returns their sum
*  If it receives "PROD <x> <y>" it returns their product
*/
int ParseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // we must have at least 4 args
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  // init auto memory for created strings
  RedisModule_AutoMemory(ctx);
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

void initQueue(struct Queue *queue){
	queue->len = 0;
	queue->first = NULL;
	queue->last = NULL;
}

int replyWith_queueInfo(RedisModuleCtx *ctx, struct RQueueObject *rqueue){
	RedisModule_ReplyWithArray(ctx,3);
	RedisModule_ReplyWithLongLong(ctx, rqueue->nextid - 1);
	RedisModule_ReplyWithLongLong(ctx, rqueue->undelivered.len);
	RedisModule_ReplyWithLongLong(ctx, rqueue->delivered.len);

	return REDISMODULE_OK;
}

/**
 * Return info on a given queue
 */
int infoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
	int type = RedisModule_KeyType(key);
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, ERRORMSG_EMPTYKEY);
	}

	if(RedisModule_ModuleTypeGetType(key) != RQueueRedisType){
		return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	return replyWith_queueInfo(ctx, RedisModule_ModuleTypeGetValue(key));
}
/**
 * mq.push <key> <msg1> [ <msg2> [...]]
 * Pushes 1 or more items into key
 */
int pushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	struct RQueueObject *rqueue;
	struct Msg *newmsg;
	int count = argc - 2; // count of new messages being pushed

	if (argc < 3) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

   RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
   int type = RedisModule_KeyType(key);
   
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		// Key doesn't exist. Create...
		rqueue = RedisModule_Alloc(sizeof(*rqueue));
		rqueue->nextid = 1;
		initQueue(&rqueue->undelivered);
		initQueue(&rqueue->delivered);
		RedisModule_ModuleTypeSetValue(key, RQueueRedisType, rqueue);
	} else if(RedisModule_ModuleTypeGetType(key) == RQueueRedisType){
		// key exists. Get and update
		rqueue = RedisModule_ModuleTypeGetValue(key);
	} else {
		// Key exists, but it's not an RQueueObject!!!
		return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	// create the new message
	newmsg = RedisModule_Alloc(sizeof(*newmsg) * count);

	// Init the new nodes
	for(int i = 0, j = 1; i < count; i++, j++){
		newmsg[i].id = rqueue->nextid++;
		newmsg[i].created = mstime();
		newmsg[i].lastDelivery = 0;
		newmsg[i].deliveries = 0;
		newmsg[i].next = (
			j < count ?
			&newmsg[j] :
			NULL
		);
		newmsg[i].value = RedisModule_CreateStringFromString(NULL, argv[2 + i]);
	}

	if(rqueue->undelivered.first == NULL){
		rqueue->undelivered.first = &newmsg[0];
	} else {
		rqueue->undelivered.last->next = &newmsg[0];
	}
	rqueue->undelivered.last = &newmsg[count - 1];
	rqueue->undelivered.len += count;

	return replyWith_queueInfo(ctx, RedisModule_ModuleTypeGetValue(key));
}

int rangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);

	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
   
	if(RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx,ERRORMSG_EMPTYKEY);
	}

	if(RedisModule_ModuleTypeGetType(key) != RQueueRedisType){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	struct RQueueObject *rqueue = RedisModule_ModuleTypeGetValue(key);

	struct Msg *cur = (
		redis_stricmp(argv[2], "PENDING") == 0 ?
		rqueue->delivered.first :
		rqueue->undelivered.first
	);

	long total = 0;
	RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
	while(cur){
		RedisModule_ReplyWithArray(ctx,3);
		RedisModule_ReplyWithLongLong(ctx, cur->id);
		RedisModule_ReplyWithLongLong(ctx, cur->created);
		//RedisModule_ReplyWithLongLong(ctx, cur->lastDelivery);
		//RedisModule_ReplyWithLongLong(ctx, cur->deliveries);
		RedisModule_ReplyWithString(ctx, cur->value);
		total++;
		cur = cur->next;
	}
	RedisModule_ReplySetArrayLength(ctx, total);
	
	return REDISMODULE_OK;
}

/**
 * Usage: POP [ COUNT n ] <key1> [ <key2> [ ... ] ]
 */
int popCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	if (argc < 2) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	// Retrieve the key content
	RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
   
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithNull(ctx);
	}
	
	if(RedisModule_ModuleTypeGetType(key) != RQueueRedisType){
		return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	struct RQueueObject *rqueue = RedisModule_ModuleTypeGetValue(key);

	if(rqueue->undelivered.first == NULL){
		return RedisModule_ReplyWithNull(ctx);
	}

	struct  Msg *topop = rqueue->undelivered.first;
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

	// Reply
	RedisModule_ReplyWithArray(ctx, 3);
	RedisModule_ReplyWithLongLong(ctx, topop->id);
	RedisModule_ReplyWithLongLong(ctx, topop->created);
	RedisModule_ReplyWithString(ctx, topop->value);

	return REDISMODULE_OK;
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
    	.rdb_load = QueueRDBLoad,
    	.rdb_save = QueueRDBSave,
    	.aof_rewrite = QueueAofRewrite,
    	.free = QueueReleaseObject
	};

   RQueueRedisType = RedisModule_CreateDataType(
		ctx,
		"Queue-EDU",
		RQUEUE_ENCODING_VERSION,
		&tm
	);

   if (RQueueRedisType == NULL) return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"mq.push", pushCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx,"mq.pop", popCommand,"write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	// register xq.info - the default registration syntax
	if (RedisModule_CreateCommand(ctx, "mq.info", infoCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

	// register xq.parse - the default registration syntax
	if (RedisModule_CreateCommand(ctx, "mq.range", rangeCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

	// register the unit test
	RMUtil_RegisterWriteCmd(ctx, "mq.test", TestModule);
	

	return REDISMODULE_OK;
}
