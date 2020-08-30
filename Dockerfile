FROM redis:5.0.5

RUN apt update && apt install -y build-essential
COPY redis.conf /usr/local/etc/redis/redis.conf
RUN touch /var/log/redis.log && chown redis:redis /var/log/redis.log
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]
