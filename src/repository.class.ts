import {DynamoDB} from "aws-sdk";
import generatorToArray from "./generator-to-array";

import DocumentClient = DynamoDB.DocumentClient;

export type EntityGenerator<Entity> = () => Promise<Entity>;

const hash = "HASH";
const range = "RANGE";

export interface ISearchInput {
	IndexName?: DocumentClient.IndexName;
	Select?: DocumentClient.Select;
	Limit?: DocumentClient.PositiveIntegerObject;
	ConsistentRead?: DocumentClient.ConsistentRead;
	ScanIndexForward?: DocumentClient.BooleanObject;
	ExclusiveStartKey?: DocumentClient.Key;
	ReturnConsumedCapacity?: DocumentClient.ReturnConsumedCapacity;
	ProjectionExpression?: DocumentClient.ProjectionExpression;
	FilterExpression?: DocumentClient.ConditionExpression;
	KeyConditionExpression?: DocumentClient.KeyExpression;
	ExpressionAttributeNames?: DocumentClient.ExpressionAttributeNameMap;
	ExpressionAttributeValues?: DocumentClient.ExpressionAttributeValueMap;
	TotalSegments?: DocumentClient.ScanTotalSegments;
	Segment?: DocumentClient.ScanSegment;
}

export interface ICountInput {
	IndexName?: DocumentClient.IndexName;
	ConsistentRead?: DocumentClient.ConsistentRead;
	ReturnConsumedCapacity?: DocumentClient.ReturnConsumedCapacity;
	FilterExpression?: DocumentClient.ConditionExpression;
	KeyConditionExpression?: DocumentClient.KeyExpression;
	ExpressionAttributeNames?: DocumentClient.ExpressionAttributeNameMap;
	ExpressionAttributeValues?: DocumentClient.ExpressionAttributeValueMap;
}

export interface IDynamoDBRepository<Entity> {
	get(key: DocumentClient.Key): Promise<Entity>;
	getList(ids: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity>>;
	search(input: ISearchInput): EntityGenerator<Entity>;
	getEntityKey(e: Entity): DocumentClient.Key;
	getEntityId(e: Entity): string;
	addToCache(e: Entity): void;
	clear(): void;
}

export interface IGenerator<Entity> {
	(): Promise<Entity>;
	toArray(): Promise<Entity[]>;
}

export class DynamoDBRepository<Entity> implements IDynamoDBRepository<Entity> {

	private static isQueryInput(input: any): input is DocumentClient.QueryInput {
		return input.KeyConditionExpression !== undefined;
	}

	private queueFreePromise: Promise<any>;
	private unMarshal: (item: DocumentClient.AttributeMap) => Entity;
	private cache: Map<string, Promise<Entity>>;
	private hashKey: string;
	private rangeKey: string;

	constructor(
		private dc: DocumentClient,
		private tableName: string,
		private keySchema: DocumentClient.KeySchema,
		unMarshal?: (item: DocumentClient.AttributeMap) => Entity,
	) {
		this.cache = new Map();
		this.unMarshal = unMarshal === undefined ? (i: any) => i : unMarshal;
		this.hashKey = keySchema.find((k) => k.KeyType === hash).AttributeName;
		const rangeSchema = keySchema.find((k) => k.KeyType === range);
		this.queueFreePromise = Promise.resolve();
		if (rangeSchema) {
			this.rangeKey = rangeSchema.AttributeName;
		}
	}

	public async get(key: DocumentClient.Key) {
		const stringifiedKey = this.stringifyKey(key);
		if (false === this.cache.has(stringifiedKey)) {
			this.cache.set(stringifiedKey, this.loadEntity(key));
		}

		return this.cache.get(stringifiedKey);
	}

	public async getList(keys: DocumentClient.Key[]) {
		const result = new Map<DocumentClient.Key, Entity>();
		const notCachedKeys: DocumentClient.Key[] = [];
		for (const key of keys) {
			const stringifiedKey = this.stringifyKey(key);
			if (this.cache.has(stringifiedKey)) {
				result.set(key, await this.cache.get(stringifiedKey));
			} else if (notCachedKeys.some((k) => sameKey(k, key)) === false) {
				notCachedKeys.push(key);
			}
		}
		if (notCachedKeys.length > 0) {
			const response = await this.loadEntities(notCachedKeys);
			for (const key of notCachedKeys) {
				const stringifiedKey = this.stringifyKey(key);
				const entity = response.get(key);
				if (entity === undefined) {
					this.cache.set(stringifiedKey, Promise.resolve(undefined));
				} else {
					await this.addToCache(entity);
				}
				result.set(key, await this.cache.get(stringifiedKey));
			}
		}

		return result;
	}

	public search(input: ISearchInput): IGenerator<Entity> {
		const getNextEntity = this.searchInDocumentClient(input);
		const mustAddToCache = input.ProjectionExpression === undefined;
		const generator = (async () => {
			const entity = await getNextEntity();
			if (entity === undefined) {
				return;
			}
			const id = this.getEntityId(entity);
			if (mustAddToCache) {
				await this.addToCache(entity);
			}

			return this.cache.get(id);
		}) as IGenerator<Entity>;
		generator.toArray = generatorToArray;

		return generator;
	}

	public async count(input: ICountInput) {
		const documentClientInput = Object.assign({}, input, {TableName: this.tableName, Select: "COUNT"});
		const inputIsQuery = DynamoDBRepository.isQueryInput(input);
		const response = await (inputIsQuery ?
			new Promise<DocumentClient.QueryOutput>(
				(rs, rj) => this.dc.query(documentClientInput, (err, res) => err ? rj(err) : rs(res)),
			) :
			new Promise<DocumentClient.ScanOutput>(
				(rs, rj) => this.dc.scan(documentClientInput, (err, res) => err ? rj(err) : rs(res)),
			));

		return response.Count;
	}

	public clear() {
		this.cache.clear();
	}

	public stringifyKey(key: DocumentClient.Key) {
		return `[${key[this.hashKey]}][${key[this.rangeKey]}]`;
	}

	public getEntityId(entity: Entity) {
		return this.stringifyKey(this.getEntityKey(entity));
	}

	public getEntityKey(entity: Entity) {
		const key: DocumentClient.Key = {};
		key[this.hashKey] = (entity as any)[this.hashKey];
		if (this.rangeKey) {
			key[this.rangeKey] = (entity as any)[this.rangeKey];
		}

		return key;
	}

	public async addToCache(entity: Entity) {
		const id = this.getEntityId(entity);
		if (this.cache.has(id) && await this.cache.get(id) !== undefined) {
			return;
		}
		this.cache.set(id, Promise.resolve(entity));
	}

	private searchInDocumentClient(input: ISearchInput) {

		const getNextBlock = this.buildScanBlockGenerator(input);

		let batch: any[] = [];
		let sourceIsEmpty = false;

		return async () => {
			while (batch.length === 0 && sourceIsEmpty === false) {
				const dynamoResponse = await getNextBlock();
				batch = dynamoResponse.Items;
				sourceIsEmpty = dynamoResponse.LastEvaluatedKey === undefined;
			}
			if (batch.length === 0) {
				return;
			}

			return this.unMarshal(batch.shift());
		};
	}

	private async loadEntity(Key: DocumentClient.Key) {
		const input: DocumentClient.GetItemInput = {
			Key,
			TableName: this.tableName,
		};
		const response = await this.asyncGet(input);

		return response.Item === undefined ? undefined : this.unMarshal(response.Item);
	}

	private async loadEntities(keys: DocumentClient.Key[]) {
		const input: DocumentClient.BatchGetItemInput = {
			RequestItems: {
				[this.tableName]: {Keys: keys},
			},
		};
		const response = await new Promise<DocumentClient.BatchGetItemOutput>(
			(rs, rj) => this.dc.batchGet(input, (err, res) => err ? rj(err) : rs(res)),
		);
		const result = new Map<DocumentClient.Key, Entity>();
		for (const item of response.Responses[this.tableName]) {
			const entity = this.unMarshal(item);
			result.set(keys.find((k) => this.stringifyKey(k) === this.getEntityId(entity)), entity);
		}

		return result;
	}

	private buildScanBlockGenerator(input: ISearchInput) {
		const documentClientInput: DocumentClient.ScanInput | DocumentClient.QueryInput = Object.assign(
			{},
			input,
			{TableName: this.tableName},
		);
		const inputIsQuery = DynamoDBRepository.isQueryInput(documentClientInput);
		let lastEvaluatedKey: any;
		let sourceIsEmpty = false;
		if (input.ExclusiveStartKey !== undefined) {
			lastEvaluatedKey = input.ExclusiveStartKey;
		}

		return async () => {
			if (sourceIsEmpty) {
				return;
			}
			const blockInput = Object.assign(documentClientInput, {ExclusiveStartKey: lastEvaluatedKey});
			const response = await (inputIsQuery ? this.asyncQuery(blockInput) : this.asyncScan(blockInput));
			lastEvaluatedKey = response.LastEvaluatedKey;
			if (undefined === lastEvaluatedKey) {
				sourceIsEmpty = true;
			}

			return response;
		};
	}

	private asyncQuery(input: DocumentClient.QueryInput) {
		return new Promise<DocumentClient.QueryOutput>(
			(rs, rj) => this.dc.query(input, (err, res) => err ? rj(err) : rs(res)),
		);
	}

	private asyncScan(input: DocumentClient.ScanInput) {
		return new Promise<DocumentClient.ScanOutput>(
			(rs, rj) => this.dc.scan(input, (err, res) => err ? rj(err) : rs(res)),
		);
	}

	private asyncGet(input: DocumentClient.GetItemInput) {
		return new Promise<DocumentClient.GetItemOutput>(
			(rs, rj) => this.dc.get(input, (err, res) => err ? rj(err) : rs(res)),
		);
	}
}

function sameKey(key1: DocumentClient.Key, key2: DocumentClient.Key) {
	return Object.keys(key1).every((k) => key2[k] === key1[k]);
}
