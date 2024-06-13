import * as redis from 'redis';
class ScheduledWorker {
    queueId;
    pollInterval = 1000;
    redisClient;
    callback;
    pollIntervalId = null;
    running = false;
    constructor(options) {
        if (typeof options !== 'object') {
            throw new TypeError('No constructor settings specified');
        }
        if (options.queueId != null) {
            this.queueId = options.queueId;
        }
        else {
            throw new TypeError('Invalid queue ID specified');
        }
        if (options.redisClient != null) {
            this.redisClient = options.redisClient;
        }
        else {
            throw new TypeError('Invalid redis connection options');
        }
        if (options.callback != null) {
            this.callback = options.callback;
        }
        else {
            throw new TypeError('Invalid callback function specified');
        }
        if (options.pollInterval != null && options.pollInterval > 0) {
            this.pollInterval = options.pollInterval;
        }
    }
    start() {
        this.pollIntervalId = setInterval(this.poll.bind(this), this.pollInterval);
    }
    stop() {
        if (this.pollIntervalId) {
            clearInterval(this.pollIntervalId);
            this.pollIntervalId = null;
        }
    }
    async poll() {
        if (this.running)
            return;
        this.running = true;
        const now = new Date().getTime();
        let flag = false;
        do {
            flag = false;
            try {
                await this.redisClient.watch(this.queueId);
                let datas = await this.redisClient.zRangeByScore(this.queueId, 0, now, {
                    LIMIT: { count: 1, offset: 0 },
                });
                if (datas.length > 0) {
                    let data = datas.shift();
                    if (!data)
                        break;
                    let results = await this.redisClient
                        .multi()
                        .zRem(this.queueId, data)
                        .exec();
                    if (results?.length && results[0] == 1) {
                        flag = true;
                        await this.callback(data);
                    }
                }
            }
            catch (err) {
                if (!(err instanceof redis.WatchError)) {
                    throw err;
                }
            }
        } while (flag);
        this.running = false;
    }
    async addToRedis(value, scheduledAt) {
        await this.redisClient.zAdd(this.queueId, { score: scheduledAt, value });
    }
    async add(...datas) {
        return Promise.all(datas.map(async (data) => {
            await this.addToRedis(data, 0);
        }));
    }
    addDelayed(delayMs, ...data) {
        if (delayMs && delayMs <= 0) {
            throw new TypeError('`delayMs` must be a positive integer');
        }
        let scheduledAt = new Date(new Date().getTime() + delayMs);
        return this.addAt(scheduledAt, ...data);
    }
    async addAt(at, ...datas) {
        if (!(at && at instanceof Date)) {
            throw new TypeError('`at` must be a valid date');
        }
        else if (datas == null || !Array.isArray(datas)) {
            throw new TypeError('No value provided for `datas`');
        }
        const scheduledAt = at.getTime();
        let taskIds = await Promise.all(datas.map(async (data) => {
            await this.addToRedis(data, scheduledAt);
        }));
        return taskIds;
    }
}
export default ScheduledWorker;
//# sourceMappingURL=ScheduledWorker.js.map