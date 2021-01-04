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

	constructor(
		private readonly keySchemas: {[tableName: string]: {hash: string, range?: string}},
	) {
		this.resumed = Promise.resolve();
		this.stepMode = false;
		this.resumedEventEmitter = new EventEmitter();
		this.shouldFail = false;
		this.collections = {};
	}

	public get(
		input: DocumentClient.GetItemInput,
	) {
		return {promise: async () => {
			await this.awaitFlush();
			this.guardShouldFail();
			const hashKey = input.Key[this.keySchemas[input.TableName].hash];
			const rangeKey = input.Key[this.keySchemas[input.TableName].range];
			this.ensureHashKey(input.TableName, hashKey);
			const marshaled = this.collections[input.TableName][hashKey][rangeKey];
			const a = marshaled ? JSON.parse(marshaled) : undefined;

			return ({Item: a});
		}};
	}

	public async set(tableName: TableName, item: DocumentClient.AttributeMap) {
		await this.put({TableName: tableName, Item: item}).promise();
	}

	public async getByKey<IEntity>(tableName: TableName, key: DocumentClient.Key): Promise<IEntity> {
		return (await this.get({TableName: tableName, Key: key}).promise()).Item;
	}

	public batchGet(
		input: DocumentClient.BatchGetItemInput,
	) {
		return {promise: async () => {
			await this.awaitFlush();
			this.guardShouldFail();
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
			return response
		}};
	}

	public scan(
		input: DocumentClient.ScanInput,
	) {
		return {promise: async () => {
				await this.awaitFlush();
				this.guardShouldFail();
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
			return response;
			}};
	}

	public put(
		input: DocumentClient.PutItemInput,
	) {
		return {promise: async () => {
				await this.awaitFlush();
				this.guardShouldFail();
				const hashKey = input.Item[this.keySchemas[input.TableName].hash];
				const rangeKey = input.Item[this.keySchemas[input.TableName].range];
				this.ensureHashKey(input.TableName, hashKey);
				this.collections[input.TableName][hashKey][rangeKey] = JSON.stringify(input.Item);
			return ({})
			}};
	}

	public delete(
		input: DocumentClient.DeleteItemInput,
	) {


		return {promise: async () => {
				const hashKey = input.Key[this.keySchemas[input.TableName].hash];
				const rangeKey = input.Key[this.keySchemas[input.TableName].range];
				this.collections[input.TableName][hashKey][rangeKey] = undefined;
			return ({})
			}};
	}

	public flush() {
		this.resumedEventEmitter.emit("resumed");
		this.resumed = new Promise((rs) => this.resumedEventEmitter.once("resumed", rs));
	}

	public failOnCall(error?: Error) {
		this.shouldFail = true;
		this.error = error;
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

	private guardShouldFail() {
		if (this.shouldFail === false) {
			return;
		}

		throw this.error !== undefined ? this.error : new Error("Repository error");
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
