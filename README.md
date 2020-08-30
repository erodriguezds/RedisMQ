# RedisMQ (still in progress!!! Please don't use it... yet)

A Redis module for implementing better and truly reliable message broking within Redis.

## The ReliableQueue data structure

This module it's all about the ReliableQueue: a single and simple data structure to implement reliable, and practical, queues in Redis.

### What about the "Reliable queue pattern"?

The "Reliable queue pattern" described at https://redis.io/commands/rpoplpush only offers a solution for half of the problem (I'm not going to explain it here. Please read the pattern, if you haven't, and you'll know what can be done with the pattern). The other half without a (decent) solution is: what do we do with the messages/tasks that were never removed from the "processing" queue (because, for example, the consumer failed to process it)? You might be thinking: "Easy, dummy, you need a different consumer for consuming the "failed" messages frrom the "processing" queue"... to which I'd reply: "Great, dummy, but... how can you tell how much time those failed messages have been hanging in there in the "processing" queue? Maybe those messages were just poped-out from our actual queue, and the consumers are still processing them... If you "recover" (POP) those "maybe failed / maybe still processing" messages , and process them, you might end up having the messages being processed more than once!!!"... To which you might start having weird ideas like having the messages enveloped inside a JSON, to which you could add all the metadata you whish... and then.... forget it, my friend... all that shitty workarounds you're thinking right now should be handled by your message broker, in our case: Redis... and Redis is not even an actual message broker!!! until now...

### Gimme the details about this "ReliableQueue" data structure!!!



### 1. redismodule.h

The only file you really need to start writing Redis modules. Either put this path into your module's include path, or copy it. 

Notice: This is an up-to-date copy of it from the Redis repo.

### 2. LibRMUtil 

A small library of utility functions and macros for module developers, including:

* Easier argument parsing for your commands.
* Testing utilities that allow you to wrap your module's tests as a redis command.
* `RedisModuleString` utility functions (formatting, comparison, etc)
* The entire `sds` string library, lifted from Redis itself.
* A generic scalable Vector library. Not redis specific but we found it useful.
* A few other helpful macros and functions.
* `alloc.h`, an include file that allows modules implementing data types to implicitly replace the `malloc()` function family with the Redis special allocation wrappers.

It can be found under the `rmutil` folder, and compiles into a static library you link your module against.    

### 3. An example Module

A minimal module implementing a few commands and demonstarting both the Redis Module API, and use of rmutils.

You can treat it as a template for your module, and extned its code and makefile.

**It includes 3 commands:**

* `EXAMPLE.PARSE` - demonstrating rmutil's argument helpers.
* `EXAMPLE.HGETSET` - an atomic HGET/HSET command, demonstrating the higher level Redis module API.
* `EXAMPLE.TEST` - a unit test of the above commands, demonstrating use of the testing utilities of rmutils.  
  
### 4. Documentation Files:

1. [API.md](API.md) - The official manual for writing Redis modules, copied from the Redis repo. 
Read this before starting, as it's more than an API reference.

2. [FUNCTIONS.md](FUNCTIONS.md) - Generated API reference documentation for both the Redis module API, and LibRMUtil.

3. [TYPES.md](TYPES.md) - Describes the API for creating new data structures inside Redis modules, 
copied from the Redis repo.

4. [BLOCK.md](BLOCK.md) - Describes the API for blocking a client while performing asynchronous tasks on a separate thread.


# Quick Start Guide

Here's what you need to do to build your first module:

0. Build Redis in a build supporting modules.
1. Build librmutil and the module by running `make`. (you can also build them seperatly by running `make` in their respective dirs)
2. Run redis loading the module: `/path/to/redis-server --loadmodule ./module.so`

Now run `redis-cli` and try the commands:

```
127.0.0.1:9979> EXAMPLE.HGETSET foo bar baz
(nil)
127.0.0.1:9979> EXAMPLE.HGETSET foo bar vaz
"baz"
127.0.0.1:9979> EXAMPLE.PARSE SUM 5 2
(integer) 7
127.0.0.1:9979> EXAMPLE.PARSE PROD 5 2
(integer) 10
127.0.0.1:9979> EXAMPLE.TEST
PASS
```

Enjoy!
    
