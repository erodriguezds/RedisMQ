#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"
#include "./module.h"

static RedisModuleType *RQueueRedisType;
#define RQUEUE_ENCODING_VERSION 0

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

/*
* example.HGETSET <key> <element> <value>
* Atomically set a value in a HASH key to <value> and return its value before
* the HSET.
*
* Basically atomic HGET + HSET
*/
int HGetSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // we need EXACTLY 4 arguments
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // open the key and make sure it's indeed a HASH and not empty
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH &&
      RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  // get the current value of the hash element
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "HGET", "ss", argv[1], argv[2]);
  RMUTIL_ASSERT_NOERROR(ctx, rep);

  // set the new value of the element
  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "HSET", "sss", argv[1], argv[2], argv[3]);
  RMUTIL_ASSERT_NOERROR(ctx, srep);

  // if the value was null before - we just return null
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  // forward the HGET reply to the client
  RedisModule_ReplyWithCallReply(ctx, rep);
  return REDISMODULE_OK;
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
		newmsg[i].delivered = 0;
		newmsg[i].next = (
			j < count ?
			&newmsg[j] :
			NULL
		);
		newmsg[i].value = argv[2 + i]; 
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

/**
 * Pops 1 or more messages from the given key
 */
int popCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
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
		newmsg[i].delivered = 0;
		newmsg[i].next = (
			j < count ?
			&newmsg[j] :
			NULL
		);
		newmsg[i].value = argv[2 + i]; 
	}

	if(rqueue->undelivered.first == NULL){
		rqueue->undelivered.first = &newmsg[0];
	} else {
		rqueue->undelivered.last->next = &newmsg[0];
	}
	rqueue->undelivered.last = &newmsg[count - 1];
	rqueue->undelivered.len += count;

	RedisModule_ReplyWithArray(ctx,3);
	RedisModule_ReplyWithLongLong(ctx, rqueue->nextid - 1);
	RedisModule_ReplyWithLongLong(ctx, rqueue->undelivered.len);
	RedisModule_ReplyWithLongLong(ctx, rqueue->delivered.len);

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

	// register xq.parse - the default registration syntax
	if (RedisModule_CreateCommand(ctx, "mq.info", infoCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

  // register xq.hgetset - using the shortened utility registration macro
  RMUtil_RegisterWriteCmd(ctx, "mq.hgetset", HGetSetCommand);

  // register the unit test
  RMUtil_RegisterWriteCmd(ctx, "mq.test", TestModule);

	

	return REDISMODULE_OK;
}
