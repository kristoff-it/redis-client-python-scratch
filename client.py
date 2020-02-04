# A Redis client from scratch

# TESTS:
# 1. SET banana hello
# 2. GET banana
# 3. INCR banana
# 3. SET banana "hello world"
# 4. SET banana "hello \n world"
import asyncio

class RedisClient:
	async def connect(self, host, port):
		self.r, self.w = await asyncio.open_connection(host, port)

	async def set(self, key, value):
		self.w.write(f"SET {key} \"{value}\"\r\n".encode())
		await self.w.drain()

		return await self._read_reply()

	async def get(self, key):
		self.w.write(f"GET {key}\r\n".encode())
		await self.w.drain()
		
		return await self._read_reply()

	async def incr(self, key):
		self.w.write(f"INCR {key}\r\n".encode())
		await self.w.drain()
		
		return await self._read_reply()

	async def send(self, *args):
		resp_args = "".join([f"${len(x)}\r\n{x}\r\n" for x in args])
		self.w.write(f"*{len(args)}\r\n{resp_args}".encode())
		await self.w.drain()

		return await self._read_reply()


	async def _read_reply(self):
		tag = await self.r.read(1)

		if tag == b'$':
			length = b''
			ch = b''

			while ch != b'\n':
				ch = await self.r.read(1)
				length += ch

			total_length = int(length[:-1]) + 2

			result = b''
			while len(result) < total_length:
				result += await self.r.read(total_length - len(result))

			return result[:-2].decode()
		if tag == b':':
			result = b''
			ch = b''

			while ch != b'\n':
				ch = await self.r.read(1)
				result += ch
			return int(result[:-1].decode())

		if tag == b'-':
			result = b''
			ch = b''

			while ch != b'\n':
				ch = await self.r.read(1)
				result += ch
			raise Exception(result[:-1].decode())
		if tag == b'+':
			result = b''
			ch = b''

			while ch != b'\n':
				ch = await self.r.read(1)
				result += ch
			return result[:-1].decode()
		else:
			msg = await self.r.read(100)
			raise Exception(f"Unknown tag: {tag}, msg: {msg}")

async def runner(client):
	for _ in range(1000):
		await client.send("incr", "banana")

async def main():
	print("Hello asyncio!")
	client = RedisClient()
	await client.connect("localhost", 6379)

	# asyncio.create_task(runner(client))
	# asyncio.create_task(runner(client))
	# asyncio.create_task(runner(client))

	print(await client.set("banana", "1"))
	print(await client.get("banana"))
	print(await client.incr("banana"))
	print(await client.send("hset", "banana", "f1", "v1", 'f2', 'v2'))

if __name__ == '__main__':
	asyncio.run(main())