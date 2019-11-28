import {DynamoDB} from "aws-sdk";
import {DynamoEntityManager} from "dynamo-entity-manager";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import RepositoryCached, {ICachedRepositoryTableConfig} from "../cached/repository.class";
import IEntityGenerator from "../generator.interface";
import IQueryInput from "../query-input.interface";
import IRepositoryTableConfig from "../repository-table-config.interface";
import IScanInput from "../scan-input.interface";
import ManagedRepositoryGenerator from "./generator.class";

export default class RepositoryManaged<Entity> extends RepositoryCached<Entity> {

	protected config: IRepositoryTableConfig<Entity>;

	constructor(
		config: ICachedRepositoryTableConfig<Entity>,
		dynamo: PoweredDynamo,
		private entityManager: DynamoEntityManager,
		eventEmitter?: EventEmitter,
	) {
		super(dynamo, config, eventEmitter);
	}

	public async get(key: DynamoDB.DocumentClient.Key) {
		const result = await super.get(key);
		this.entityManager.track(this.config.tableName, result, this.versionOf(result));

		return result;
	}

	public async getList(keys: DynamoDB.DocumentClient.Key[]) {
		const list = await super.getList(keys);
		for (const entity of list.values()) {
			this.entityManager.track(this.config.tableName, entity, this.versionOf(entity));
		}

		return list;
	}

	public delete(e: Entity) {
		this.entityManager.delete(this.config.tableName, e);
	}

	public async trackNew(e: Entity) {
		await super.addToCache(e);
		this.entityManager.trackNew(this.config.tableName, e);
	}

	public async track(e: Entity) {
		await super.addToCache(e);
		this.entityManager.track(this.config.tableName, e, this.versionOf(e));
	}

	public scan(input: IScanInput): IEntityGenerator<Entity> {
		return new ManagedRepositoryGenerator<Entity>(
			this,
			super.scan(input),
		);
	}

	public query(input: IQueryInput): IEntityGenerator<Entity> {
		return new ManagedRepositoryGenerator<Entity>(
			this,
			super.query(input),
		);
	}
}
