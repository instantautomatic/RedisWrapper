from django.conf import settings
import pickle
import redis

class RedisWrapper:
    def __get_key(self, key):
        prefix = getattr(settings, 'REDIS_KEY_PREFIX', '')
        return '%s_%s' % (prefix, key)
    
    def __get_client(self):
        host = getattr(settings, 'REDIS_HOST', 'localhost')
        port = getattr(settings, 'REDIS_PORT', 6379)
        db = getattr(settings, 'REDIS_DB', 0)
        return redis.StrictRedis(host=host, port=port, db=db)
    
    def get(self, key):
        c = self.__get_client()
        k = self.__get_key(key)
        v = c.get(k)
        if v is None:
            return None
        return pickle.loads(v)
        
    def set(self, key, value, timeout=None):
        c = self.__get_client()
        v = pickle.dumps(value)
        k = self.__get_key(key)
        result = c.set(k, v)
        if timeout is not None:
            c.expire(k, timeout)
        return result
