"""
Author: Yeap Jie Shen
"""
import os
import redis

class RedisClient:
    """
        This class is a client class for Redis services
        Redis service can be started / stopped here without the need to do in terminal
    """
    def __init__(self, host = 'localhost', port = 6379, db = 0, sudo_password = 'password', start_now = True):
        """
         DEFAULT host          -> localhost
         DEFAULT port          -> 6379
         DEFAULT db            -> 0
         DEFAULT sudo_password -> password

         Redis functionalities is achieved through self.redis_client
         Redis service status is achieved through os.system()
        """
        self.host = host
        self.port = port
        self.db = db
        self.sudo_password = sudo_password
        self.redis_client = redis.Redis(host = self.host, port = self.port)

        if start_now:
            self.start_service()

    def start_service(self):
        """
            Start a Redis service in the terminal with the password set
        """
        os.system(f'echo {self.sudo_password} | sudo -S systemctl start redis')

    def stop_service(self):
        """
            Stop a Redis service in the terminal with the password set
        """
        os.system(f'echo {self.sudo_password} | sudo -S systemctl stop redis')

    def check_status(self):
        """
            Check the status of a Redis services in the termianl with the password set
        """
        return os.popen(f'echo {self.sudo_password} | sudo -S systemctl status redis')

    def get_keys(self, pattern = '*'):
        """
            Get the keys annotated by pattern, if available
        """
        return self.redis_client.keys(pattern)

    def exists_key(self, key):
        """
            Check if the provided key exists
        """
        return self.redis_client.exists(key)

    def get_value(self, key):
        """
            Get the value with the provided key
        """
        return self.redis_client.get(key)

    def set_key_value(self, key, value, seconds = None):
        """
            Set the key-value pair with the provided key and value
        """
        status = self.redis_client.set(key, value)

        if seconds:
            self.set_key_expire(key, seconds)

        return status

    def delete_key_value(self, key):
        """
            Delete the key-value pair with the provided key
        """
        return self.redis_client.delete(key)

    def mget_values(self, keys):
        """
            Get multiple values with the provided keys
        """
        return self.redis_client.mget(keys)

    def mset_keys_values(self, key_value_map):
        """
            Set multiple key-value pairs with the provided map of keys and values
        """
        return self.redis_client.mset(key_value_map)

    def set_key_expire(self, key, seconds):
        """
            Set time-to-live (TTL) of the specified key 
        """
        return self.redis_client.expire(key, seconds)

    def get_ttl(self, key):
        """
            Get the time-to-live (TTL) of the specified key
        """
        return self.redis_client.ttl(key)

    def remove_key_expire(self, key):
        """
            Remove time-to-live (TTL) of the specified key
        """
        return self.redis_client.persist(key)