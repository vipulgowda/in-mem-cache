import time
import asyncio
import hashlib
import httpx

# This is test file I am updating
class DistributedCache:
    def __init__(self, num_nodes=5):
        self.nodes = [Cache() for _ in range(num_nodes)]

    def get_node_key(self, key):
        print(key)
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return self.nodes[hash_value % len(self.nodes)]

    async def get(self, key):
        await self.getAll()
        node = self.get_node_key(key)
        res = await node.get(key)
        print(res)

    async def put(self, key, value, ttl=20):
        node = self.get_node_key(key)
        print("New print")
        await node.put(key, value, ttl)

    async def getAll(self):
        for node in self.nodes:
            await node.getAll()

    async def cleanup(self):
        for node in self.nodes:
            await node.getAll()
            await node.cleanup()


class LRUCache:
    def __init__(self):
        self.csize = 2
        self.dq = []
        self.ma = {}

    def refer(self, x):
        if x not in self.ma.keys():
            if len(self.dq) == self.csize:
                last = self.dq[-1]
                self.dq.pop()
                del self.ma[last]
            else:
                del self.dq[self.ma[x]]
            self.dq.insert(0, x)
            self.ma[x] = 0


class Cache:
    def __init__(self):
        self.cache = {}
        self.lru_cache = []
        self.maxSize = 5

    async def getAll(self):
        print(self.cache.keys())

    async def get(self, key):
        time.sleep(1)
        if key in self.cache:
            if self.cache[key]["expiry"] < time.time():
                self.delete(key)
            print("Getting the key", key)
            print(self.cache[key])

    async def put(self, key, value, ttl=0):
        time.sleep(1)
        expiration_time = time.time() + ttl
        if len(self.lru_cache) < self.maxSize:
            self.lru_cache.append(key)
        while len(self.cache) > self.maxSize:
            last = self.lru_cache[-1]
            self.delete(last)
            self.lru_cache.pop()
        if key not in self.cache:
            async with httpx.AsyncClient(timeout=60.0) as client:
                value = await run_url(client, f'https://api.punkapi.com/v2/beers?page={value + 1}')
                print("API resp ==>", value)
                self.cache[key] = {"value": value, "expiry": expiration_time}
                print("Inserted the key", key)

    async def delete(self, key):
        if key in self.cache:
            time.sleep(1)
            print("Deleted the key", key)
            del self.cache[key]

    async def cleanup(self):
        print("Inside Cleanup")
        expired_keys = [key for key, item in self.cache.items()
                        if item["expiry"] < time.time()]
        for key in expired_keys:
            print("Cleaning up key: ", key)
            await self.delete(key)


async def run_url(client, url):
    resp = await client.get(url, follow_redirects=True)
    print(resp.text)
    if resp.text:
        print(resp.text)
        return resp.text


async def simulate_concurrent_requests(cache, num_requests):
    tasks = []
    # file = open("../size.txt").read()
    for i in range(num_requests):
        key = f"key{i}"
        tasks.append(cache.put(key, i, 30))
        # if i % 5 == 0:
        #   tasks.append(cache.get(key))
        # if i % 4 == 0:
        #   tasks.append(cache.cleanup())
    s = time.perf_counter()
    result = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - s
    print(elapsed)
    res = await cache.get('key2')
    print(res)
    return result


async def main():
    cache = DistributedCache(5)
    return await simulate_concurrent_requests(cache, 5)


def runAsync():
    return (asyncio.run(main()))


def printAsync():
    return "Hello World"


if __name__ == '__main__':
    runAsync()
