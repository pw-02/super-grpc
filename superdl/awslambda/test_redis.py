import redis

endpoint='54.190.161.64'
client = redis.StrictRedis(host=endpoint, port='6378')
print(client.get('a0e3eae2f7e5f47def3e0261a907e84e4a50e3ed'))