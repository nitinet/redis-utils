import * as  redis from 'redis';
import * as uuid from 'uuid';

interface IOptions {
	queueId: string,
	redisClient: redis.RedisClientType<any, any, any>;
	callback: (data: any) => void;
	pollInterval?: number;
}

class QueueWorker {

	queueId: string = null;
	pollInterval: number = 1000;
	redisClient: redis.RedisClientType<any, any, any> = null;
	callback: (data: any) => void = null;
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
		clearInterval(this.pollIntervalId);
		this.pollIntervalId = null;
	}

	/**
	 * Polls redis for tasks.
	 */
	private async poll() {
		const now = new Date().getTime();
		let task: string = null;
		do {
			try {
				task = await this.redisClient.lPop(this.queueId);

				if (task != null) {
					// Process tasks
					let item = JSON.parse(task);
					this.callback(item);
				}
			} catch (err) {
				throw TypeError('Invalid redis Operation');
			}
		} while (task != null);
	}

	/**
	* Add a scheduled task
	* @param  {any[]} datas data to be scheduled
	*/
	async add(...datas: any[]) {
		return Promise.all(datas.map(async data => {
			let value = JSON.stringify(data);
			await this.redisClient.rPush(this.queueId, value);
		}));
	}

}

export default QueueWorker;