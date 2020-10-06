FROM redis:latest as builder

RUN apt update && apt install -y build-essential

ADD . /usr/src/redisrq
WORKDIR /usr/src/redisrq
RUN make

#---------------------------------------------------------------------------------------------- 
# Package the runner
FROM redis:latest

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"
COPY --from=builder /usr/src/redisrq/bin/redisrq.so "$LIBDIR"

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redisrq.so"]
