#!/bin/sh

redis-cli -p 6376 FLUSHDB
redis-cli -p 6379 FLUSHDB

redis-cli -p 6374 shutdown
redis-cli -p 6375 shutdown
redis-cli -p 6376 shutdown
redis-cli -p 6377 shutdown
redis-cli -p 6378 shutdown
redis-cli -p 6379 shutdown
