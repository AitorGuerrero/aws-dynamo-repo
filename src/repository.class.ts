import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import {PoweredDynamo} from "powered-dynamo";
import EntityGenerator from "./generator.class";
import IEntityGenerator from "./generator.interface";
import IQueryInput from "./query-input.interface";
import IRepositoryTableConfig from "./repository-table-config.interface";
import IScanInput from "./scan-input.interface";

import DocumentClient = DynamoDB.DocumentClient;

export default class DynamoDBRepository<Entity> {

	public readonly eventEmitter: EventEmitter;

	protected readonly config: IRepositoryTableConfig<Entity>;

	private entityVersions = new Map<Entity, number>();

	constructor(
		protected dc: PoweredDynamo,
		config: IRepositoryTableConfig<Entity>,
		eventEmitter?: EventEmitter,
	) {
		this.config = Object.assign({
			marshal: (e: Entity) => JSON.parse(JSON.stringify(e)) as DocumentClient.AttributeMap,
			unMarshal: (e: DocumentClient.AttributeMap) => JSON.parse(JSON.stringify(e)) as Entity,
		}, config);
		this.eventEmitter = eventEmitter || new EventEmitter();
	}

	public async get(Key: DocumentClient.Key): Promise<Entity> {
		const input: DocumentClient.GetItemInput = {
			Key,
			TableName: this.config.tableName,
		};
		const response = await this.dc.get(input);
		if (response.Item !== undefined) {
			const entity = this.config.unMarshal(response.Item);
			if (this.config.versionKey) {
				this.entityVersions.set(entity, response.Item[this.config.versionKey]);
			}

			return entity;
		}

		return undefined;
	}

	public async getList(keys: DocumentClient.Key[]) {
		const result = new Map<DocumentClient.Key, Entity>();
		const items = await this.dc.getList(this.config.tableName, keys);
		for (const key of items.keys()) {
			const item = items.get(key);
			const entity = this.config.unMarshal(item);
			this.entityVersions.set(entity, item[this.config.versionKey]);
			result.set(key, entity);
		}

		return result;
	}

	public scan(input: IScanInput): IEntityGenerator<Entity> {
		return new EntityGenerator<Entity>(
			this.dc.scan(Object.assign({
				TableName: this.config.tableName,
			}, input)),
			this.config,
			(entity, version) => this.entityVersions.set(entity, version),
		);
	}

	public query(input: IQueryInput): IEntityGenerator<Entity> {
		return new EntityGenerator<Entity>(
			this.dc.query(Object.assign({
				TableName: this.config.tableName,
			}, input)),
			this.config,
			(entity, version) => this.entityVersions.set(entity, version),
		);
	}

	protected versionOf(e: Entity): number {
		return this.entityVersions.get(e);
	}

	protected registerEntityVersion(e: Entity, version: number) {
		this.entityVersions.set(e, version);
	}
}
