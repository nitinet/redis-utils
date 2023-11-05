import * as  redis from 'redis';

import * as types from './types.js';

interface IOptions {
	queueId: string,
	redisClient: redis.RedisClientType<any, any, any>;
	callback: types.CallbackType;
	pollInterval?: number;
}

class ScheduledWorker {
	queueId: string;
	pollInterval: number = 1000;
	redisClient: redis.RedisClientType<any, any, any>;
	callback: types.CallbackType;
	pollIntervalId: NodeJS.Timeout | null = null;

	running: boolean = false;

	constructor(options: IOptions) {
		if (typeof options !== 'object') {
			throw new TypeError('No constructor settings specified');
		}

		// Check ID
		if (options.queueId != null) {
			this.queueId = options.queueId;
		} else {
			throw new TypeError('Invalid queue ID specified');
		}

		if (options.redisClient != null) {
			this.redisClient = options.redisClient;
		} else {
			throw new TypeError('Invalid redis connection options');
		}

		// Callback function for all delayed tasks
		if (options.callback != null) {
			this.callback = options.callback;
		} else {
			throw new TypeError('Invalid callback function specified');
		}

		// Poll Interval - how often to poll redis (Default: 1000ms)
		if (options.pollInterval != null && options.pollInterval > 0) {
			this.pollInterval = options.pollInterval;
		}
	}

	/**
	 * Start polling.
	 */
	start() {
		this.pollIntervalId = setInterval(this.poll.bind(this), this.pollInterval);
	}

	/**
	 * Stops polling.
	 */
	stop() {
		if (this.pollIntervalId) {
			clearInterval(this.pollIntervalId);
			this.pollIntervalId = null;
		}
	}

	/**
	 * Polls redis for tasks.
	 */
	private async poll() {
		if (this.running) return;

		this.running = true;
		const now = new Date().getTime();
		let flag = false;
		do {
			flag = false;
			try {
				await this.redisClient.watch(this.queueId);
				let datas = await this.redisClient.zRangeByScore(this.queueId, 0, now, { LIMIT: { count: 1, offset: 0 } });

				if (datas.length > 0) {
					let data = datas.shift();
					if (!data) break;

					let results = await this.redisClient.multi()
						.zRem(this.queueId, data)
						.exec();

					if (results?.length && results[0] == 1) {
						// Process tasks
						flag = true;
						await this.callback(data);
					}
				}
			} catch (err) {
				if (!(err instanceof redis.WatchError)) {
					throw err;
				}
			}
		} while (flag);
		this.running = false;
	}

	/**
	 * Adds a task to redis.
	 */
	private async addToRedis(value: string, scheduledAt: number) {
		await this.redisClient.zAdd(this.queueId, { score: scheduledAt, value });
	}

	/**
 * Add a scheduled task
 * @param  {string[]} datas data to be scheduled
 */
	async add(...datas: string[]) {
		return Promise.all(datas.map(async data => {
			await this.addToRedis(data, 0);
		}));
	}

	/**
	 * Add a scheduled task
	 * @param  {number} delayMs delay time in milli sec
	 * @param  {string[]} data data to be scheduled
	 */
	addDelayed(delayMs: number, ...data: string[]) {
		// Validate `delayMs`
		if (delayMs && delayMs <= 0) {
			throw new TypeError('`delayMs` must be a positive integer');
		}

		// Set time to execute
		let scheduledAt = new Date(new Date().getTime() + delayMs);

		return this.addAt(scheduledAt, ...data);
	}

	/**
	 * Adds multiple tasks at scheduled time
	 * @param  {Date} at scheduled time
	 * @param  {string[]} datas array of data
	 */
	async addAt(at: Date, ...datas: string[]) {
		// Validate `at`
		if (!(at && at instanceof Date)) {
			throw new TypeError('`at` must be a valid date');
		} else if (datas == null || !Array.isArray(datas)) {
			throw new TypeError('No value provided for `datas`');
		}

		// Set time to execute
		const scheduledAt = at.getTime();

		let taskIds = await Promise.all(datas.map(async data => {
			await this.addToRedis(data, scheduledAt);
		}));

		return taskIds;
	}

}

export default ScheduledWorker;