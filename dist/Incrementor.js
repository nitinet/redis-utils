import * as redis from 'redis';
class Incrementor {
    redisOpts;
    client;
    count;
    constructor(redisOpts, count) {
        this.redisOpts = redisOpts;
        this.client = redis.createClient(this.redisOpts);
        this.count = count ?? 1000;
    }
    async loadPool(key) {
        let sizeKey = key + '-size';
        let data = await this.client.get(sizeKey);
        let currIdx = 0;
        if (data)
            currIdx = Number.parseInt(data);
        Array.from({ length: this.count }).forEach(async (v, i) => {
            await this.client.rPush(key, (currIdx + i).toString());
        });
        await this.client.set(sizeKey, (currIdx + this.count).toString());
    }
    async getId(key) {
        let idStr = await this.client.lPop(key);
        let id;
        if (idStr) {
            id = Number.parseInt(idStr);
        }
        else {
            await this.loadPool(key);
            id = await this.getId(key);
        }
        return id;
    }
    async addId(key, id) {
        await this.client.rPush(key, id.toString());
    }
}
export default Incrementor;
//# sourceMappingURL=Incrementor.js.map