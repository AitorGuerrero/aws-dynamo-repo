import {DynamoDB} from "aws-sdk";
import PoweredDynamo from "powered-dynamo";
import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "./generator.class";
import IncompleteIndexGenerator from "./incomplete-index-generator";
import RepositoryTableConfig, {ProjectionType} from "./repository-table-config.interface";
import SearchResult from "./search-result.interface";

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
			const entity = this.config.unMarshal(response.Item);
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
			const entity = this.config.unMarshal(item);
			this.entityVersions.set(entity, item[this.config.versionKey]);
			result.set(key, entity);
		}

		return result;
	}

	public async scan(input: ScanInput): Promise<SearchResult<Entity>> {
		return this.buildEntityGenerator(
			input,
			await this.poweredDynamo.scan(Object.assign({
				TableName: this.config.tableName,
			}, input)),
		);
	}

	public async query(input: QueryInput): Promise<SearchResult<Entity>> {
		return this.buildEntityGenerator(
			input,
			await this.poweredDynamo.query(Object.assign({
				TableName: this.config.tableName,
			}, input)),
		);
	}

	protected versionOf(e: Entity): number | undefined {
		return this.entityVersions.get(e);
	}

	protected registerEntityVersion(e: Entity, version: number): void {
		this.entityVersions.set(e, version);
	}

	private buildEntityGenerator(input: QueryInput | ScanInput, generator: IGenerator): SearchResult<Entity> {
		if (this.requestInputIsOfIncompleteIndex(input)) {
			return  new IncompleteIndexGenerator<Entity>(
				this,
				generator,
				this.config,
				(entity, version) => this.registerEntityVersion(entity, version),
			);
		}

		return new EntityGenerator<Entity>(
			generator,
			this.config,
			(entity, version) => this.entityVersions.set(entity, version),
		);
	}

	private requestInputIsOfIncompleteIndex(input: QueryInput | ScanInput): boolean {
		return input.IndexName
			&& this.config.secondaryIndexes
			&& this.config.secondaryIndexes[input.IndexName]
			&& this.config.secondaryIndexes[input.IndexName].ProjectionType !== ProjectionType.all;
	}
}
