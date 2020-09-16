FROM redis:latest as builder

RUN apt update && apt install -y build-essential

ADD . /usr/src/redismq
WORKDIR /usr/src/redismq
RUN make

#---------------------------------------------------------------------------------------------- 
# Package the runner
FROM redis:latest

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"
COPY --from=builder /usr/src/redismq/bin/redismq.so "$LIBDIR"

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redismq.so"]
