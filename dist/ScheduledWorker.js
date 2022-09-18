import * as uuid from 'uuid';
class Item {
    constructor(id, scheduledAt, data) {
        this.id = id;
        this.scheduledAt = scheduledAt;
        this.data = data;
    }
}
class ScheduledWorker {
    constructor(options) {
        this.queueId = null;
        this.pollInterval = 1000;
        this.redisClient = null;
        this.callback = null;
        this.pollIntervalId = null;
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
        clearInterval(this.pollIntervalId);
        this.pollIntervalId = null;
    }
    async poll() {
        const now = new Date().getTime();
        let tasks = null;
        do {
            try {
                await this.redisClient.watch(this.queueId);
                tasks = await this.redisClient.zRangeByScore(this.queueId, 0, now, { LIMIT: { count: 1, offset: 0 } });
                if (tasks.length > 0) {
                    let task = tasks[0];
                    let results = await this.redisClient.multi()
                        .zRem(this.queueId, task)
                        .exec();
                    if (results && results[0] !== null) {
                        let item = JSON.parse(task);
                        this.callback(item.data, item.id);
                    }
                }
            }
            catch (err) {
                throw TypeError('Invalid redis Operation');
            }
        } while (tasks != null && tasks.length > 0);
    }
    async addToRedis(task) {
        let value = JSON.stringify(task);
        await this.redisClient.zAdd(this.queueId, { score: task.scheduledAt, value });
    }
    async add(...datas) {
        return Promise.all(datas.map(async (data) => {
            const taskId = uuid.v4();
            let task = new Item(uuid.v4(), 0, data);
            await this.addToRedis(task);
            return taskId;
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
            const taskId = uuid.v4();
            let task = new Item(uuid.v4(), scheduledAt, data);
            await this.addToRedis(task);
            return taskId;
        }));
        return taskIds;
    }
}
export default ScheduledWorker;
//# sourceMappingURL=ScheduledWorker.js.map