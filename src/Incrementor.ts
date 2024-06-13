import * as redis from 'redis';

class Incrementor {
  client: redis.RedisClientType;
  count: number;

  constructor(client: redis.RedisClientType, count?: number) {
    this.client = client;
    this.count = count ?? 1000;
  }

  private async loadPool(key: string) {
    let indexKey = key + '-index';
    let data = await this.client.get(indexKey);
    let currIdx: number = 0;
    if (data) currIdx = Number.parseInt(data);

    await Promise.all(
      Array.from({ length: this.count }).map(async (v, i) => {
        await this.client.rPush(key, (currIdx + i).toString());
      })
    );
    await this.client.set(indexKey, (currIdx + this.count).toString());
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
