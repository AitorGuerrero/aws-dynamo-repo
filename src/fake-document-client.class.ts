import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";

import DocumentClient = DynamoDB.DocumentClient;

export type TableName = string;

export default class FakeDocumentClient {

	public stepMode: boolean;
	public readonly collections: {[tableName: string]: {[hashKey: string]: {[rangeKey: string]: string}}};
	private resumed: Promise<any>;
	private resumedEventEmitter: EventEmitter;
	private shouldFail: boolean;
	private error: Error;
	private hashKey: string;
	private rangeKey: string;

	constructor(
		private readonly keySchemas: {[tableName: string]: {hash: string, range?: string}},
	) {
		this.resumed = Promise.resolve();
		this.stepMode = false;
		this.resumedEventEmitter = new EventEmitter();
		this.shouldFail = false;
		this.collections = {};
	}

	public async get(
		input: DocumentClient.GetItemInput,
		cb: (err?: Error, result?: DocumentClient.GetItemOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const hashKey = input.Key[this.keySchemas[input.TableName].hash];
		const rangeKey = input.Key[this.keySchemas[input.TableName].range];
		this.ensureHashKey(input.TableName, hashKey);
		const marshaled = this.collections[input.TableName][hashKey][rangeKey];
		cb(null, {Item: marshaled ? JSON.parse(this.collections[input.TableName][hashKey][rangeKey]) : undefined});
	}

	public async set(tableName: TableName, item: DocumentClient.AttributeMap) {
		await new Promise((rs) => this.put({TableName: tableName, Item: item}, () => rs()));
	}

	public getByKey<IEntity>(tableName: TableName, key: DocumentClient.Key): IEntity {
		return new Promise((rs) => this.get({TableName: tableName, Key: key}, (err, result) => rs(result.Item))) as any;
	}

	public async batchGet(
		input: DocumentClient.BatchGetItemInput,
		cb: (err?: Error, result?: DocumentClient.BatchGetItemOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const response: DocumentClient.BatchGetItemOutput = {Responses: {}};
		for (const tableName in input.RequestItems) {
			response.Responses[tableName] = [];
			for (const request of input.RequestItems[tableName].Keys) {
				const hashKey = request[this.keySchemas[tableName].hash];
				const rangeKey = request[this.keySchemas[tableName].range];
				this.ensureHashKey(tableName, hashKey);
				const item = this.collections[tableName][hashKey][rangeKey];
				if (item !== undefined) {
					response.Responses[tableName].push(JSON.parse(item));
				}
			}
		}
		cb(null, response);
	}

	public async scan(
		input: DocumentClient.ScanInput,
		cb: (err?: Error, result?: DocumentClient.ScanOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const response: DocumentClient.ScanOutput = {Items: []};
		const startKey = this.getStartKey(input.TableName, input.ExclusiveStartKey);
		const existingHashKeys = Object.keys(this.collections[input.TableName]);
		let hashKey = startKey.hash;
		let rangeKey = startKey.range;
		let rangeKeys = Object.keys(this.collections[input.TableName][hashKey]);
		while (this.collections[input.TableName][hashKey] !== undefined) {
			while (rangeKey !== undefined && this.collections[input.TableName][hashKey][rangeKey] !== undefined) {
				response.Items.push(JSON.parse(this.collections[input.TableName][hashKey][rangeKey]));
				rangeKey = rangeKeys[rangeKeys.indexOf(rangeKey) + 1];
			}
			hashKey = existingHashKeys[existingHashKeys.indexOf(hashKey) + 1];
			if (hashKey === undefined) {
				break;
			}
			rangeKeys = Object.keys(this.collections[input.TableName][hashKey]);
			rangeKey = rangeKeys[0];
		}
		if (hashKey !== undefined) {
			response.LastEvaluatedKey = {
				[this.keySchemas[input.TableName].hash]: hashKey,
				[this.keySchemas[input.TableName].range]: rangeKey,
			};
		}

		cb(null, response);
	}

	public async query(
		input: DocumentClient.QueryInput,
		cb: (err?: Error, result?: DocumentClient.QueryOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const response: DocumentClient.ScanOutput = {Items: []};
		const startKey = this.getStartKey(input.TableName, input.ExclusiveStartKey);
		const hashKeys = Object.keys(this.collections[input.TableName]);
		let hashKey = startKey.hash;
		let rangeKey = startKey.range;
		let rangeKeys = Object.keys(this.collections[input.TableName][hashKey]);
		while (this.collections[input.TableName][hashKey] !== undefined) {
			while (rangeKey !== undefined && this.collections[input.TableName][hashKey][rangeKey] !== undefined) {
				response.Items.push(JSON.parse(this.collections[input.TableName][hashKey][rangeKey]));
				rangeKey = rangeKeys[rangeKeys.indexOf(rangeKey) + 1];
			}
			hashKey = hashKeys[hashKeys.indexOf(hashKey) + 1];
			if (hashKey === undefined) {
				break;
			}
			rangeKeys = Object.keys(this.collections[input.TableName][hashKey]);
			rangeKey = rangeKeys[0];
		}
		if (hashKey !== undefined) {
			response.LastEvaluatedKey = {
				[this.keySchemas[input.TableName].hash]: hashKey,
				[this.keySchemas[input.TableName].range]: rangeKey,
			};
		}

		cb(null, response);
	}

	public async update(
		input: DocumentClient.UpdateItemInput,
		cb: (err?: Error, result?: DocumentClient.UpdateItemOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const item = await this.getByKey(input.TableName, input.Key);
		const updates: {k: string, v: any}[] = /UPDATE/.test(input.UpdateExpression) ?
			/UPDATE ([^,]*)/.exec(input.UpdateExpression)[1]
				.split(" AND ").map((s) => s.replace(" ", "").split("="))
				.map((s) => ({k: s[0], v: s[1]})) :
			[];
		const deletes: string[] = /DELETE/.test(input.UpdateExpression) ?
			/DELETE ([^,]*)/.exec(input.UpdateExpression)[1]
				.split(" AND ").map((s) => s.replace(" ", "")) :
			[];

		for (const update of updates) {
			let toUpdate: any = item;
			for (const k of update.k.split(".")) {
				const realName = input.ExpressionAttributeNames[k];
				if (typeof toUpdate[realName] !== "object") {
					toUpdate[realName] = input.ExpressionAttributeValues[update.v];
					continue;
				}
				toUpdate = toUpdate[realName];
			}
		}
		for (const deleteField of deletes) {
			let toDelete: any = item;
			for (const k of deleteField.split(".")) {
				const realName = input.ExpressionAttributeNames[k];
				if (typeof toDelete[realName] !== "object") {
					toDelete[realName] = undefined;
					continue;
				}
				toDelete = toDelete[realName];
			}
		}
		await this.set(input.TableName, item);

		cb(null, {});
	}

	public async put(
		input: DocumentClient.PutItemInput,
		cb: (err?: Error, result?: DocumentClient.PutItemOutput) => any,
	) {
		await this.awaitFlush();
		this.guardShouldFail(cb);
		const hashKey = input.Item[this.keySchemas[input.TableName].hash];
		const rangeKey = input.Item[this.keySchemas[input.TableName].range];
		this.ensureHashKey(input.TableName, hashKey);
		this.collections[input.TableName][hashKey][rangeKey] = JSON.stringify(input.Item);
		cb(null, {});
	}

	public async delete(
		input: DocumentClient.DeleteItemInput,
		cb: (err?: Error, result?: DocumentClient.DeleteItemOutput) => any,
	) {
		const hashKey = input.Key[this.keySchemas[input.TableName].hash];
		const rangeKey = input.Key[this.keySchemas[input.TableName].range];
		this.collections[input.TableName][hashKey][rangeKey] = undefined;
		cb(null, {});
	}

	public flush() {
		this.resumedEventEmitter.emit("resumed");
		this.resumed = new Promise((rs) => this.resumedEventEmitter.once("resumed", rs));
	}

	public failOnCall(error?: Error) {
		this.shouldFail = true;
		this.error = error;
	}

	public transactWrite(
		input: DocumentClient.TransactWriteItemsInput,
		cb: (err?: Error, result?: DocumentClient.TransactWriteItemsOutput) => any,
	) {
		input.TransactItems
			.filter((i) => i !== undefined)
			.forEach((i) => {
				if (i.Delete) {
					const hashKey = i.Delete.Key[this.keySchemas[i.Delete.TableName].hash];
					const rangeKey = i.Delete.Key[this.keySchemas[i.Delete.TableName].range];
					this.collections[i.Delete.TableName][hashKey][rangeKey] = undefined;
				} else if (i.Put) {
					const hashKey = i.Put.Item[this.keySchemas[i.Put.TableName].hash];
					const rangeKey = i.Put.Item[this.keySchemas[i.Put.TableName].range];
					this.ensureHashKey(i.Put.TableName, hashKey);
					this.collections[i.Put.TableName][hashKey][rangeKey] = JSON.stringify(i.Put.Item);
				}
			});
		cb(null, {});
	}

	private getStartKey(tableName: string, exclusiveStartKey: DocumentClient.Key) {
		let range: string;
		let hash: string;

		if (exclusiveStartKey === undefined) {
			hash = Object.keys(this.collections[tableName])[0];
			range = Object.keys(this.collections[tableName][hash])[0];
			return {hash, range};
		}

		hash = exclusiveStartKey[this.keySchemas[tableName].hash];
		const rangeKeys = Object.keys(this.collections[tableName][exclusiveStartKey[this.keySchemas[tableName].hash]]);
		range = rangeKeys[rangeKeys.indexOf(exclusiveStartKey[this.keySchemas[tableName].range]) + 1];
		if (range === undefined) {
			const hashKeys = Object.keys(this.collections[tableName]);
			hash = hashKeys[hashKeys.indexOf(hash) + 1];
			range = Object.keys(this.collections[tableName][hash])[0];
		}

		return {hash, range};
	}

	private async awaitFlush() {
		if (this.stepMode) {
			await this.resumed;
		}
	}

	private guardShouldFail(cb: (err: Error) => any) {
		if (this.shouldFail === false) {
			return;
		}
		const error = this.error !== undefined ? this.error : new Error("Repository error");
		cb(error);
		throw error;
	}

	private ensureHashKey(tableName: string, hashKey: string) {
		if (this.collections[tableName] === undefined) {
			this.collections[tableName] = {};
		}
		if (this.collections[tableName][hashKey] === undefined) {
			this.collections[tableName][hashKey] = {};
		}
	}
}
