import * as  redis from 'redis';

class Incrementor {
	redisOpts: redis.RedisClientOptions<any, any, any>;
	client: redis.RedisClientType<any, any, any>;
	count: number;

	constructor(redisOpts: redis.RedisClientOptions<any, any, any>, count?: number) {
		this.redisOpts = redisOpts;
		this.client = redis.createClient(this.redisOpts)
		this.count = count ?? 1000;
	}

	private async loadPool(key: string) {
		let sizeKey = key + '-size';
		let data = await this.client.get(sizeKey);
		let currIdx: number = 0;
		if (data) currIdx = Number.parseInt(data);

		Array.from({ length: this.count }).forEach(async (v, i) => {
			await this.client.rPush(key, (currIdx + i).toString());
		});
		await this.client.set(sizeKey, (currIdx + this.count).toString());
	}

	async getId(key: string) {
		let idStr = await this.client.lPop(key);
		let id: number;
		if (idStr) {
			id = Number.parseInt(idStr);
		} else {
			await this.loadPool(key);
			id = await this.getId(key);
		}
		return id;
	}

	async addId(key: string, id: number) {
		await this.client.rPush(key, id.toString());
	}

}

export default Incrementor;
