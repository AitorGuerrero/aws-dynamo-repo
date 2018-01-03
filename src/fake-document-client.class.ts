import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";

import DocumentClient = DynamoDB.DocumentClient;

const hash = "HASH";
const range = "RANGE";

export default class FakeDocumentClient<Entity> {

	public maxResponses = 2;
	private resumed: Promise<any>;
	private resumedEventEmitter: EventEmitter;
	private shouldFail: boolean;
	private error: Error;
	private hashKey: string;
	private rangeKey: string;

	constructor(
		private collection: Map<string, DocumentClient.AttributeMap>,
		public tableName: string,
		public keySchema: DocumentClient.KeySchema,
	) {
		this.resumed = Promise.resolve();
		this.resumedEventEmitter = new EventEmitter();
		this.shouldFail = false;
		this.hashKey = keySchema.find((k) => k.KeyType === hash).AttributeName;
		const rangeSchema = keySchema.find((k) => k.KeyType === range);
		if (rangeSchema) {
			this.rangeKey = rangeSchema.AttributeName;
		}
	}

	public async get(
		input: DocumentClient.GetItemInput,
		cb: (err?: Error, result?: DocumentClient.GetItemOutput) => any,
	) {
		await this.resumed;
		this.guardShouldFail(cb);
		cb(null, {Item: this.collection.get(this.stringifyKey(input.Key))});
	}

	public getByKey(key: DocumentClient.Key) {
		return this.collection.get(this.stringifyKey(key));
	}

	public set(item: DocumentClient.AttributeMap) {
		this.collection.set(this.stringifyKey(this.constructItemKey(item)), item);
	}

	public async batchGet(
		input: DocumentClient.BatchGetItemInput,
		cb: (err?: Error, result?: DocumentClient.BatchGetItemOutput) => any,
	) {
		await this.resumed;
		this.guardShouldFail(cb);
		const items: DocumentClient.ItemList = [];
		for (const request of input.RequestItems[this.tableName].Keys) {
			const item = this.collection.get(this.stringifyKey(request));
			if (item !== undefined) {
				items.push(item);
			}
		}
		cb(null, {Responses: {[this.tableName]: items}});
	}

	public async scan(
		input: DocumentClient.ScanInput,
		cb: (err?: Error, result?: DocumentClient.ScanOutput) => any,
	) {
		await this.resumed;
		this.guardShouldFail(cb);
		const allItems = Array.from(this.collection.values());
		const start = input.ExclusiveStartKey !== undefined ?
			allItems.indexOf(this.collection.get(this.stringifyKey(input.ExclusiveStartKey)) as any) + 1 :
			0;
		const items = allItems.slice(start, start + this.maxResponses);
		const LastEvaluatedKey = items[items.length - 1] !== allItems[allItems.length - 1]
			? this.constructItemKey(items[items.length - 1])
			: undefined;
		cb(null, {Items: items, LastEvaluatedKey});
	}

	public async query(
		input: DocumentClient.QueryInput,
		cb: (err?: Error, result?: DocumentClient.QueryOutput) => any,
	) {
		await this.resumed;
		this.guardShouldFail(cb);
		const allItems = Array.from(this.collection.values());
		const start = input.ExclusiveStartKey !== undefined ?
			allItems.indexOf(this.collection.get(this.stringifyKey(input.ExclusiveStartKey)) as any) + 1 :
			0;
		const items = allItems.slice(start, start + this.maxResponses);
		const LastEvaluatedKey = items[items.length - 1] !== allItems[allItems.length - 1]
			? this.constructItemKey(items[items.length - 1])
			: undefined;
		cb(null, {Items: items, LastEvaluatedKey});
	}

	public async put(
		input: DocumentClient.PutItemInput,
		cb: (err?: Error, result?: DocumentClient.PutItemOutput) => any,
	) {
		await this.resumed;
		this.guardShouldFail(cb);
		this.collection.set(this.stringifyKey(this.constructItemKey(input.Item)), input.Item as any);
		cb(null, {});
	}

	public stop() {
		this.resumed = new Promise((rs) => this.resumedEventEmitter.once("resumed", () => rs()));
	}

	public resume() {
		this.resumedEventEmitter.emit("resumed");
	}

	public failOnCall(error?: Error) {
		this.shouldFail = true;
		this.error = error;
	}

	private constructItemKey(item: DocumentClient.AttributeMap) {
		const key: DocumentClient.Key = {};
		key[this.hashKey] = item[this.hashKey];
		if (this.rangeKey) {
			key[this.rangeKey] = item[this.rangeKey];
		}

		return key;
	}

	private guardShouldFail(cb: (err: Error) => any) {
		if (this.shouldFail === false) {
			return;
		}
		const error = this.error !== undefined ? this.error : new Error("Repository error");
		cb(error);
		throw error;
	}

	private stringifyKey(key: DocumentClient.Key) {
		return `[${key[this.hashKey]}][${key[this.rangeKey]}]`;
	}
}
