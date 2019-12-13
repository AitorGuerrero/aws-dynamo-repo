import {DynamoDB} from "aws-sdk";
import {DynamoEntityManager} from "dynamo-entity-manager";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import DynamoCachedRepository, {ICachedRepositoryTableConfig} from "../cached/repository.class";
import IQueryInput from "../query-input.interface";
import IRepositoryTableConfig from "../repository-table-config.interface";
import IScanInput from "../scan-input.interface";
import ManagedRepositoryGenerator from "./generator.class";

export default class DynamoManagedRepository<Entity> extends DynamoCachedRepository<Entity> {

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

	public async delete(e: Entity) {
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

	public async scan(input: IScanInput) {
		return new ManagedRepositoryGenerator<Entity>(
			this,
			await super.scan(input),
		);
	}

	public async query(input: IQueryInput) {
		return new ManagedRepositoryGenerator<Entity>(
			this,
			await super.query(input),
		);
	}
}
