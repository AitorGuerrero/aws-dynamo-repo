import {DynamoDB} from "aws-sdk";
import PoweredDynamo from "powered-dynamo";
import RepositoryTableConfig, {ProjectionType} from "./repository-table-config.interface";

import DocumentClient = DynamoDB.DocumentClient;

export type QueryInput = Omit<DynamoDB.DocumentClient.QueryInput, 'TableName'>;
export type ScanInput = Omit<DynamoDB.DocumentClient.ScanInput, 'TableName'>;

export default class DynamoRepository<Entity> {

	protected readonly config: RepositoryTableConfig<Entity>;

	private entityVersions = new Map<Entity, number>();

	constructor(
		protected poweredDynamo: PoweredDynamo,
		config: RepositoryTableConfig<Entity>,
	) {
		this.config = Object.assign({
			marshal: (e: Entity) => JSON.parse(JSON.stringify(e)) as DocumentClient.AttributeMap,
			unMarshal: (e: DocumentClient.AttributeMap) => JSON.parse(JSON.stringify(e)) as Entity,
		}, config);
	}

	public async get(Key: DocumentClient.Key): Promise<Entity | undefined> {
		const input: DocumentClient.GetItemInput = {
			Key,
			TableName: this.config.tableName,
		};
		const response = await this.poweredDynamo.get(input)
		if (response.Item !== undefined) {
			const entity = this.config.unMarshal!(response.Item);
			if (this.config.versionKey) {
				this.entityVersions.set(entity, response.Item[this.config.versionKey]);
			}

			return entity;
		}

		return undefined;
	}

	public async getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity | undefined>> {
		const result = new Map<DocumentClient.Key, Entity>();
		const items = await this.poweredDynamo.getList(this.config.tableName, keys);
		for (const key of items.keys()) {
			const item = items.get(key);
			if (item === undefined) {
				continue;
			}
			const entity = this.config.unMarshal!(item);
			if (this.config.versionKey) {
				this.entityVersions.set(entity, item[this.config.versionKey]);
			}
			result.set(key, entity);
		}

		return result;
	}

	public scan(input: ScanInput): AsyncGenerator<Entity> {
		return this.buildEntityGenerator(
			input,
			this.poweredDynamo.scan(Object.assign({
				TableName: this.config.tableName,
			}, input)),
		);
	}

	public query(input: QueryInput): AsyncGenerator<Entity> {
		return this.buildEntityGenerator(
			input,
			this.poweredDynamo.query(Object.assign({
				TableName: this.config.tableName,
			}, input)),
		);
	}

	protected versionOf(e: Entity): number | undefined {
		return this.entityVersions.get(e);
	}

	private buildEntityGenerator(input: QueryInput | ScanInput, generator: AsyncGenerator<DynamoDB.DocumentClient.AttributeMap>): AsyncGenerator<Entity> {
		return this.requestInputIsOfIncompleteIndex(input) ? this.incompleteIndexEntityGenerator(generator) : this.entityGenerator(generator);
	}

	private async* entityGenerator(itemGenerator: AsyncGenerator<DynamoDB.DocumentClient.AttributeMap>): AsyncGenerator<Entity> {
		for await (const item of itemGenerator) {
			const entity = this.config.unMarshal!(item);
			if (this.config.versionKey) {
				this.entityVersions.set(entity, this.versionOfEntity(item));
			}

			yield entity;
		}
	}

	private async* incompleteIndexEntityGenerator(itemGenerator: AsyncGenerator<DynamoDB.DocumentClient.AttributeMap>): AsyncGenerator<Entity> {
		for await (const item of itemGenerator) {
			yield (await this.get(this.buildKeyFromAttributeMap(item))) as Entity;
		}
	}

	private buildKeyFromAttributeMap(item: DynamoDB.DocumentClient.AttributeMap) {
		const key: DynamoDB.DocumentClient.Key = {
			[this.config.keySchema.hash]: item[this.config.keySchema.hash],
		};
		if (this.config.keySchema.range) {
			key[this.config.keySchema.range] = item[this.config.keySchema.range];
		}

		return key;
	}

	private versionOfEntity(item: DynamoDB.DocumentClient.AttributeMap) {
		if (this.config.versionKey === undefined) {
			return undefined;
		}

		return item[this.config.versionKey];
	}

	private requestInputIsOfIncompleteIndex(input: QueryInput | ScanInput): boolean {
		return input.IndexName !== undefined
			&& this.config.secondaryIndexes !== undefined
			&& this.config.secondaryIndexes[input.IndexName]
			&& this.config.secondaryIndexes[input.IndexName].ProjectionType !== ProjectionType.all;
	}
}
