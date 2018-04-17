import {DynamoDB} from "aws-sdk";
import generatorToArray from "./generator-to-array";
import getEntityKey from "./get-entity-key";

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
	getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity>>;
	search(input: ISearchInput): EntityGenerator<Entity>;
	count(input: ISearchInput): Promise<number>;
}

export interface IGenerator<Entity> {
	(): Promise<Entity>;
	toArray(): Promise<Entity[]>;
}

export class DynamoDBRepository<Entity> implements IDynamoDBRepository<Entity> {

	private static isQueryInput(input: any): input is DocumentClient.QueryInput {
		return input.KeyConditionExpression !== undefined;
	}

	private readonly _unMarshal: (item: DocumentClient.AttributeMap) => Entity;
	private readonly _hashKey: string;
	private readonly _rangeKey: string;

	constructor(
		private dc: DocumentClient,
		private tableName: string,
		private _keySchema: DocumentClient.KeySchema,
		unMarshal?: (item: DocumentClient.AttributeMap) => Entity,
	) {
		this._unMarshal = unMarshal === undefined ? (i: any) => i : unMarshal;
		this._hashKey = _keySchema.find((k) => k.KeyType === hash).AttributeName;
		const rangeSchema = _keySchema.find((k) => k.KeyType === range);
		if (rangeSchema) {
			this._rangeKey = rangeSchema.AttributeName;
		}
	}

	public async get(Key: DocumentClient.Key) {
		const input: DocumentClient.GetItemInput = {
			Key,
			TableName: this.tableName,
		};
		const response = await this.asyncGet(input);

		return response.Item === undefined ? undefined : this._unMarshal(response.Item);
	}

	public async getList(keys: DocumentClient.Key[]) {
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
			const entity = this._unMarshal(item);
			result.set(keys.find((k) => sameKey(k, getEntityKey(this._keySchema, entity))), entity);
		}

		return result;
	}

	public search(input: ISearchInput) {
		const getNextBlock = this.buildScanBlockGenerator(input);

		let batch: any[] = [];
		let sourceIsEmpty = false;

		const generator = (async () => {
			while (batch.length === 0 && sourceIsEmpty === false) {
				const dynamoResponse = await getNextBlock();
				batch = dynamoResponse.Items;
				sourceIsEmpty = dynamoResponse.LastEvaluatedKey === undefined;
			}
			if (batch.length === 0) {
				return;
			}

			return this._unMarshal(batch.shift());
		}) as IGenerator<Entity> ;
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
