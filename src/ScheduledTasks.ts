import * as  redis from 'redis';
import * as uuid from 'uuid';

interface IOptions {
	queueId: string,
	client: redis.RedisClientType;
	callback: (data: any, id: string, due: Date) => void;
	pollInterval: number;
}

class Item {
	id: string;
	scheduledAt: number;
	data: any;

	constructor(id: string, scheduledAt: number, data: any) {
		this.id = id;
		this.scheduledAt = scheduledAt;
		this.data = data;
	}
}

class ScheduledTasks {

	queueId: string = null;
	pollInterval: number = 1000;
	client: redis.RedisClientType = null;
	callback: (data: any, id: string, due: Date) => void = null;
	pollIntervalId: NodeJS.Timeout = null;

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

		if (options.client != null) {
			this.client = options.client;
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
		clearInterval(this.pollIntervalId);
		this.pollIntervalId = null;
	}

	/**
	 * Polls redis for tasks.
	 */
	private async poll() {
		const now = new Date().getTime();
		let tasks: string[] = null;
		do {
			try {
				await this.client.watch(this.queueId);
				tasks = await this.client.zRangeByScore(this.queueId, 0, now, { LIMIT: { count: 1, offset: 0 } });

				if (tasks.length > 0) {
					let task = tasks[0];
					let results = await this.client.multi()
						.zRem(this.queueId, task)
						.exec();

					if (results && results[0] !== null) {
						// Process tasks
						let item: Item = JSON.parse(task);
						this.callback(item.data, item.id, new Date(item.scheduledAt));
					}
				}
			} catch (err) {
				throw TypeError('Invalid redis Operation');
			}
		} while (tasks != null && tasks.length > 0)
	}

	/**
	 * Adds a task to redis.
	 */
	private async addToRedis(task: Item) {
		let value = JSON.stringify(task)
		await this.client.zAdd(this.queueId, { score: task.scheduledAt, value });
	}

	/**
 * Add a scheduled task
 * @param  {any[]} datas data to be scheduled
 */
	async add(...datas: any[]) {
		return Promise.all(datas.map(async data => {
			// Create unique task ID
			const taskId = uuid.v4();

			let task = new Item(uuid.v4(), 0, data);
			await this.addToRedis(task);
			return taskId;
		}));
	}

	/**
	 * Add a scheduled task
	 * @param  {number} delayMs delay time in milli sec
	 * @param  {any[]} data data to be scheduled
	 */
	addDelayed(delayMs: number, ...data: any[]) {
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
	 * @param  {any[]} datas array of data
	 */
	async addAt(at: Date, ...datas: any[]) {
		// Validate `at`
		if (!(at && at instanceof Date)) {
			throw new TypeError('`at` must be a valid date');
		} else if (datas == null || !Array.isArray(datas)) {
			throw new TypeError('No value provided for `datas`');
		}

		// Set time to execute
		const scheduledAt = at.getTime();

		let taskIds = await Promise.all(datas.map(async data => {
			// Create unique task ID
			const taskId = uuid.v4();

			let task = new Item(uuid.v4(), scheduledAt, data);
			await this.addToRedis(task);
			return taskId;
		}));

		return taskIds;
	}

}

export default ScheduledTasks;