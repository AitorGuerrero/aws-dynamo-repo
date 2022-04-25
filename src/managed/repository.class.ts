import {DynamoDB} from "aws-sdk";
import {DynamoEntityManager} from "dynamo-entity-manager";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import DynamoCachedRepository, {CachedRepositoryTableConfig} from "../cached/repository.class";
import QueryInput from "../query-input.interface";
import ScanInput from "../scan-input.interface";
import ManagedRepositoryGenerator from "./generator.class";
import {DocumentClient} from "aws-sdk/clients/dynamodb";

export default class DynamoManagedRepository<Entity, Marshaled extends DocumentClient.AttributeMap> extends DynamoCachedRepository<Entity, Marshaled> {

	constructor(
		config: CachedRepositoryTableConfig<Entity, Marshaled>,
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

	public async scan(input: ScanInput) {
		return new ManagedRepositoryGenerator<Entity, Marshaled>(
			this,
			await super.scan(input),
		);
	}

	public async query(input: QueryInput) {
		return new ManagedRepositoryGenerator<Entity, Marshaled>(
			this,
			await super.query(input),
		);
	}
}
