import {DynamoDB} from "aws-sdk";
import {DynamoEntityManager} from "dynamo-entity-manager";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import CachedRepository from "../cached/repository.class";
import {QueryInput, ScanInput} from '../repository.class';
import {TableConfig} from '../cached';

export default class Repository<Entity> extends CachedRepository<Entity> {

	constructor(
		config: TableConfig<Entity>,
		dynamo: PoweredDynamo,
		private entityManager: DynamoEntityManager,
		eventEmitter?: EventEmitter,
	) {
		super(dynamo, config, eventEmitter);
	}

	public async get(key: DynamoDB.DocumentClient.Key): Promise<Entity | undefined> {
		const result = await super.get(key);
		if (result !== undefined) {
			this.entityManager.track(this.config.tableName, result, this.versionOf(result));
		}

		return result;
	}

	public async getList(keys: DynamoDB.DocumentClient.Key[]): Promise<Map<DynamoDB.DocumentClient.Key, Entity | undefined>> {
		const list = await super.getList(keys);
		for (const entity of list.values()) {
			if (entity !== undefined) {
				this.entityManager.track(this.config.tableName, entity, this.versionOf(entity));
			}
		}

		return list;
	}

	public async delete(e: Entity): Promise<void> {
		this.entityManager.delete(this.config.tableName, e);
	}

	public async trackNew(e: Entity): Promise<void> {
		await super.addToCache(e);
		this.entityManager.trackNew(this.config.tableName, e);
	}

	public async track(e: Entity): Promise<void> {
		await super.addToCache(e);
		this.entityManager.track(this.config.tableName, e, this.versionOf(e));
	}

	public async* scan(input: ScanInput): AsyncGenerator<Entity> {
		for await (const entity of super.scan(input)) {
			await this.track(entity);
			yield entity;
		}
	}

	public async* query(input: QueryInput): AsyncGenerator<Entity> {
		for await (const entity of super.query(input)) {
			await this.track(entity);
			yield entity;
		}
	}
}
