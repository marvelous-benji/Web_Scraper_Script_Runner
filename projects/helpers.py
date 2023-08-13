import redis




pool = redis.ConnectionPool(host='localhost',db=0)



def get_redis_client(cpool=pool):

    client = redis.StrictRedis(connection_pool=cpool)

    try:
        client.ping()
    except Exception as e:
        print(e)
    return client