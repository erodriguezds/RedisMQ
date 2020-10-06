# RedisRQ 

RedisRQ, for Reliable Queues, is as Redis module that introduces a new native type into your Redis instance: the RELIABLEQ type, a queue-like structure with job/message acknowledgment and recovery/re-delivery of messages that were delivered but not acknowledged after a specified time.

This simple data strucutre will enable you to implement truly reliable, and practical, queue patterns within Redis.

If you're looking for a robust Redis-based message broker solution, take a look at [Disque](https://github.com/antirez/disque-module), a clustered message broker module for Redis.

Unlike Disque, this module **does not** aim to be a fully message broker solution. This module is just about the RELIABLEQ data structure, upon which you might implement simple message broking, without requiring a Redis cluster, neither AOF persistence.

## Quick start

```
docker run -p 6379:6379 --name redis-rq erodriguezds/redis-rq:latest
```

## Table of contents
1. [Data Structures](#data-structures)
   1. [Reliable Queue](#reliable-queue)
2. [Commands](#commands)
    1. [RQ.PUSH](#rqpush)
    2. [RQ.POP](#rqpop)
    3. [RQ.ACK](#rqack)
    4. [RQ.RECOVER](#rqrecover)
    5. [RQ.INSPECT](#rqinspect)

# Data Structures <a name="data-structures"></a>

## The "Reliable Queue"<a name="reliable-queue"></a>

The essence of this module is the "Reliable Queue" data structure ("RQUEUE" from now on). This is a new data structure registered by the module into your Redis instance as a new data type. The data structure itself is very simple: It's essentially a "Queue" (a FIFO list), composed of 2 internal linked-lists : a main list for "undelivered" (never-delivered) messages, and a 2nd list of "delivered" ("at-least-once") messages.

When you **PUSH** new elements into an RQUEUE key, they get allocated into the main "undelivered" list. A [Redis-Streams-like ID](https://redis.io/topics/streams-intro#entry-ids) is assigned and returned for every item pushed.

When you **POP** elements out of the RQUEUE, you get the ID and the payload of every poped element (as you would expect). However, the poped/returned elements don't really get deallocated from the RQUEUE internal memory. Instead, the poped elements get "moved" from the "undelivered" list into the internal "delivered" (at-least-once) list, and stand there until you **ACK**nowledge them. In more technical terms, no memory allocation or deallocation occurs... we just update some pointers inside the (already allocated) key data.

When you **ACK**nowledge an item by it's given ID, the element is then removed/deallocated from the "delivered" list, and the memory is finally freed.

If you have worked with Redis Streams, you might find all this very familiar. Well... most of the workflows and commands are heavily inspired by Redis streams... but the memory management is very different. As you might know, the Redis Stream is an append-only-log-like data structure... the memory consumed by such structure just goes up and up as you "stream" (add) more elements... in order to free some memory, you have to manually trim your log... our RQUEUE data structure is, in the other hand... well... you guessed'it.... A QUEUE!!! It only consumes memory as long as you have unprocessed/unacknowleded elements in the queue. 

## Commands

### RQ.PUSH
#### Usage: RQ.PUSH   *key*   *elem1*  [ *elem2* [ ... ] ]

Pushes 1 or more elements into the RQUEUE stored at key. If key does not exist, it is created as empty RQUEUE before performing the push operations. When key holds a value that is not a list, an error is returned.

As already implied above, it is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command. Elements are inserted one after the other to the end of the queue, from the leftmost element to the rightmost element.

### RQ.POP
#### Usage: RQ.POP  [ COUNT *count* ]  [ BLOCK  *timeout* ]  *key1*  [ *key2* [ ... ] ]

Pops one or more elements from one or more queues. If more than one queue is specified, the command will try to pop all the requested elements from the fisrt queue, then from the second queue, and so on.

### Blocking behavior

If the BLOCK variant is provided, and none of the specified queues have at least 1 undelivered element to be poped, the command will block the client until any other client pushes elements into any of the specified queues, or until the specified timeout (in milliseconds) is reached, in which case a null response is returned.

#### Returned value: Array reply

This command returns an array of elements, where every element is also a 3-elements-array with: the queue key from which the element was poped, the ID of the job/message, and the job/message body.

Example Reply:
```bash
127.0.0.1:6379> rq.pop  10  myHighPriorityQueue  myLowPriorityQueue
1) 1) "myHighPriorityQueue"
   2) "1601155608777-1"
   3) "This is the content of the job/message"
2) 1) "myHighPriorityQueue"
   2) "1601155608777-2"
   3) "This is another job/message"
3) 1) "myLowPriorityQueue"
   2) "1601155608777-3"
   3) "This is a low priority job/message"
4) 1) "myLowPriorityQueue"
   2) "1601155596824-1"
   3) "This is another low priority job/message"
```


### RQ.ACK
#### Usage: RQ.ACK   *key*   *id1*   [  *id2*  [ ... ] ]

Acknowledges the successful processing of one or more messages by its given ID's, thus removing the messages from the internal *delivered* queue.

#### Returned value: Array reply
The command returns an array with the message ID's that were actually acknowledged (removed from the *delivered* queue). Under ideal conditions, you should always get an array with exactly all the ID's you provided. If a provided ID is NOT included in the reply, it means it was acknowledged before by some other process. This is an undesirable (but unavoidable) scenario that may occur when using the RQ.RECOVER command. It may occur that a message get's delivered more than once (because, for example, some process **RECOVER**ed amessage that was still beign processed). On this scenario, the consumer that ends first the message proce

### RQ.RECOVER
#### Usage: RQ.RECOVER   *key*   *count*   *elapsed*
Recover *count* elements from *key* that were "delivered" but not acknowledged after *elapsed* milliseconds or more. These are the side effects on the recovered elements:
1. The "deliveries" counter of every recovered element gets incremented by 1.
2. The "last-delivery" timestamp of every recovered element gets reset to the current server time.
3. Every recovered element gets moved from the head of the internal "delivered" queue to the end of the same queue, in order to keep the list ordered by the "last-delivery" timestamp.

### RQ.INSPECT
#### Usage: RQ.INSPECT   *key*   [ PENDING ]   *start*   *count*

Inspects *count* elements at the "undelivered" queue, starting at *start*.
If PENDING is provided after the *key* to inspect, then the elements at the
"delivered" queue will be inspected instead.

##### Reply
Without the PENDING variant, you get an array of elemets standing at the "undelivered" queue, waiting to be poped, where every element is a 2-element-array with: the payload ID, and the payload itself:

```bash
127.0.0.1:6379> rq.inspect myreliable1 -2 2
1) 1) "1599352749159-4"
   2) "my message payload at position n - 1"
2) 1) "1599352749159-5"
   2) "my message payload at position n"
```

By using the PENDING variant, you'll get the elemets at the internal "delivered" (at-least-once) queue, also as an array of elements, where every element is a 5-element array indicating:
1. ID of the payload
2. The payload
3. Unix timestamp (in milliseconds) of the last time the element was POPed, or RECOVERed. When elemets are RECOVERed (using the RQ.RECOVER command), this timestamp gets reset.
4. Milliseconds elapsed since the element was poped or recovered. This counter is reset after a RECOVER.
5. Total deliveries: total times the element has being delivered. This counter is incremented by 1 after every RECOVER.

Example reply:
```bash
127.0.0.1:6379> rq.inspect micola PENDING 0 10
1) 1) "1599530647574-5"
   2) "msg1"
   3) (integer) 1599530978782
   4) (integer) 977698
   5) (integer) 1
2) 1) "1599530943310-2"
   2) "msg1"
   3) (integer) 1599531943097
   4) (integer) 13383
   5) (integer) 1
3) 1) "1599530943310-3"
   2) "msg2"
   3) (integer) 1599531945573
   4) (integer) 10907
   5) (integer) 1
```
