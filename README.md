# RedisMQ (still in progress!!! Please don't use it... yet)

A Redis module for implementing better and truly reliable message broking within Redis.

## The "Reliable Queue" data structure

The essence of this module is the "Reliable Queue" data structure ("RQUEUE" from now on). This is a new data structure registered by the module into your Redis instance as a new data type. The data structure itself is very simple: It's essentially a "Queue" (a FIFO list), composed of 2 internal linked-lists : a main list for "undelivered" (never-delivered) messages, and a 2nd list of "delivered" ("at-least-once") messages.

When you **PUSH** new elements into an RQUEUE key, they get allocated into the main "undelivered" list. A Redis-Streams-like ID is assigned and returned for every item pushed.

When you **POP** elements out of the RQUEUE, you get the ID and the payload of every poped element (as you would expect). However, the poped/returned elements don't really get deallocated from the RQUEUE internal memory. What really happens under the hood is that the poped/returned elements are poped out from the main "undelivered" list, and moved to the internal "delivered" (at-least-once) list, and stand there until you **ACK**nowledge them.

When you **ACK**nowledge an item by it's given ID, the element is then removed/deallocated from the "delivered" list, and the memory is freed.

If you have worked with Redis Streams, you might find all this very familiar. Well... most of the workflows and commands are heavily inspired by Redis streams... but the memory management is very different. As you might know, the Redis Stream is an append-only-log-like data structure... the memory consumed by such structure just goes up and up as you "stream" (add) more elements... in order to free some memory, you have to manually trim your log... our RQUEUE data structure, in the other hand, is... well... you guessed'it.... A QUEUE!!! It only consumes memory as long as you have unprocessed/unacknowleded elements in the queue. 

### Commands

#### MQ.PUSH
##### Usage: MQ.PUSH   *key*   *elem1*  [ *elem2* [ ... ] ]

Pushes 1 or more elements into the RQUEUE stored at key. If key does not exist, it is created as empty RQUEUE before performing the push operations. When key holds a value that is not a list, an error is returned.

As already implied above, it is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command. Elements are inserted one after the other to the end of the queue, from the leftmost element to the rightmost element.

#### MQ.POP
##### Usage: MQ.POP   *count*   *key*

Pops *count* elements from the RQUEUE stored at *key*. If key does not exists, *nil* is returned. If key is NOT an RQUEUE type, an error is returned.

##### Returned value: Array reply

This command returns an array of *count* elements, where every element is also an array of 2 sub-elements: the ID of a payload, and the paylod.

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
    
