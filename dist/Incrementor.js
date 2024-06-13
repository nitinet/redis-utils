class Incrementor {
    client;
    count;
    constructor(client, count) {
        this.client = client;
        this.count = count ?? 1000;
    }
    async loadPool(key) {
        let indexKey = key + '-index';
        let data = await this.client.get(indexKey);
        let currIdx = 0;
        if (data)
            currIdx = Number.parseInt(data);
        await Promise.all(Array.from({ length: this.count }).map(async (v, i) => {
            await this.client.rPush(key, (currIdx + i).toString());
        }));
        await this.client.set(indexKey, (currIdx + this.count).toString());
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