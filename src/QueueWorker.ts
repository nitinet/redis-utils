import * as  redis from 'redis';

import * as types from './types.js';

interface IOptions {
	queueId: string,
	redisClient: redis.RedisClientType<any, any, any>;
	callback: types.CallbackType;
	pollInterval?: number;
}

class QueueWorker {
	queueId: string;
	pollInterval: number = 1000;
	redisClient: redis.RedisClientType<any, any, any>;
	callback: types.CallbackType;
	pollIntervalId: NodeJS.Timeout | null = null;

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
		let item: string | null = null;
		do {
			try {
				item = await this.redisClient.lPop(this.queueId);

				if (item != null) {
					// Process tasks
					await this.callback(item);
				}
			} catch (err) {
				throw TypeError('Invalid redis Operation');
			}
		} while (item != null);
	}

	/**
	* Add a scheduled task
	* @param  {string[]} datas data to be scheduled
	*/
	async add(...datas: string[]) {
		return Promise.all(datas.map(async value => {
			await this.redisClient.rPush(this.queueId, value);
		}));
	}

}

export default QueueWorker;