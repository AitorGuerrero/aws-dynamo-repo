import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import {PoweredDynamo} from "powered-dynamo";
import IEntityResponse from "./entity-response.interface";
import IGenerator from "./generator.interface";
import RepositoryQueryGenerator from "./query-generator.class";
import IQueryInput from "./query-input.interface";
import IRepositoryTableConfig from "./repository-table-config.interface";
import RepositoryScanGenerator from "./scan-generator.class";
import IScanInput from "./scan-query.interface";

import DocumentClient = DynamoDB.DocumentClient;

export default class DynamoDBRepository<Entity> {

	public readonly eventEmitter: EventEmitter;

	protected readonly config: IRepositoryTableConfig<Entity>;

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

	public async get(Key: DocumentClient.Key): Promise<IEntityResponse<Entity>> {
		const input: DocumentClient.GetItemInput = {
			Key,
			TableName: this.config.tableName,
		};
		const response = await this.dc.get(input);

		return {
			entity: response.Item === undefined ? undefined : this.config.unMarshal(response.Item),
			version: this.config.versionKey ? response.Item[this.config.versionKey] : undefined,
		};
	}

	public async getList(keys: DocumentClient.Key[]) {
		const result = new Map<DocumentClient.Key, IEntityResponse<Entity>>();
		const items = await this.dc.getList(this.config.tableName, keys);
		for (const key of items.keys()) {
			const item = items.get(key);
			result.set(key, {
				entity: this.config.unMarshal(item),
				version: this.config.versionKey ? item[this.config.versionKey] : undefined,
			});
		}

		return result;
	}

	public scan(input: IScanInput): IGenerator<Entity> {
		return new RepositoryScanGenerator(
			this.dc.scan(Object.assign({
				TableName: this.config.tableName,
			}, input)),
			this.config.unMarshal,
			this.config.versionKey,
		);
	}

	public query(input: IQueryInput): IGenerator<Entity> {
		return new RepositoryQueryGenerator(
			this.dc.query(Object.assign({
				TableName: this.config.tableName,
			}, input)),
			this.config.unMarshal,
			this.config.versionKey,
		);
	}
}
