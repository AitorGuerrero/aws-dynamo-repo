import {DynamoDB} from "aws-sdk";
import {DynamoEntityManager} from "dynamo-entity-manager/src/entity-manager.class";
import {EventEmitter} from "events";
import {PoweredDynamo} from "powered-dynamo";
import RepositoryCached from "../cached/repository.class";
import IGenerator from "../generator.interface";
import IQueryInput from "../query-input.interface";
import IRepositoryTableConfig from "../repository-table-config.interface";
import IScanInput from "../scan-query.interface";
import ManagedRepositoryGenerator from "./generator.class";

export default class RepositoryManaged<Entity> extends RepositoryCached<Entity> {

	constructor(
		private tableConfig: IRepositoryTableConfig<Entity>,
		dynamo: PoweredDynamo,
		private entityManager: DynamoEntityManager,
		eventEmitter?: EventEmitter,
	) {
		super(dynamo, tableConfig, eventEmitter);
	}

	public async get(key: DynamoDB.DocumentClient.Key) {
		const entity = await super.get(key);
		this.entityManager.track(this.tableConfig.tableName, entity);

		return entity;
	}

	public async getList(keys: DynamoDB.DocumentClient.Key[]) {
		const list = await super.getList(keys);
		for (const entity of list.values()) {
			this.entityManager.track(this.tableConfig.tableName, entity);
		}

		return list;
	}

	public delete(e: Entity) {
		this.entityManager.delete(this.tableConfig.tableName, e);
	}

	public async persist(e: Entity) {
		await super.addToCache(e);
		this.entityManager.trackNew(this.tableConfig.tableName, e);
	}

	public scan(input: IScanInput): IGenerator<Entity> {
		return new ManagedRepositoryGenerator<Entity>(
			super.scan(input),
			this,
		);
	}

	public query(input: IQueryInput): IGenerator<Entity> {
		return new ManagedRepositoryGenerator<Entity>(
			super.query(input),
			this,
		);
	}
}
