class QueueWorker {
    queueId = null;
    pollInterval = 1000;
    redisClient = null;
    callback = null;
    pollIntervalId = null;
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
        clearInterval(this.pollIntervalId);
        this.pollIntervalId = null;
    }
    async poll() {
        let item = null;
        do {
            try {
                item = await this.redisClient.lPop(this.queueId);
                if (item != null) {
                    await this.callback(item);
                }
            }
            catch (err) {
                throw TypeError('Invalid redis Operation');
            }
        } while (item != null);
    }
    async add(...datas) {
        return Promise.all(datas.map(async (value) => {
            await this.redisClient.rPush(this.queueId, value);
        }));
    }
}
export default QueueWorker;
//# sourceMappingURL=QueueWorker.js.map