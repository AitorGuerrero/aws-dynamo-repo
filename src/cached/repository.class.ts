import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import {PoweredDynamo} from "powered-dynamo";
import IEntityResponse from "../entity-response.interface";
import IGenerator from "../generator.interface";
import IQueryInput from "../query-input.interface";
import IRepositoryTableConfig from "../repository-table-config.interface";
import DocumentClient = DynamoDB.DocumentClient;
import DynamoDBRepository from "../repository.class";
import IScanInput from "../scan-query.interface";
import CachedRepositoryGenerator from "./generator.class";

export interface ICachedRepositoryTableConfig<Entity> extends IRepositoryTableConfig<Entity> {
	marshal?: (e: Entity) => DocumentClient.AttributeMap;
}

export default class RepositoryCached<Entity> extends DynamoDBRepository<Entity> {

	protected static getEntityKey<Entity>(entity: Entity, tableConfig: ICachedRepositoryTableConfig<unknown>) {
		const marshaledEntity = tableConfig.marshal(entity);
		const key: DocumentClient.Key = {};
		key[tableConfig.keySchema.hash] = marshaledEntity[tableConfig.keySchema.hash];
		if (tableConfig.keySchema.range) {
			key[tableConfig.keySchema.range] = marshaledEntity[tableConfig.keySchema.range];
		}

		return key;
	}

	protected readonly cache: Map<any, Map<any, Promise<IEntityResponse<Entity>>>>;

	constructor(
		protected dynamo: PoweredDynamo,
		config: ICachedRepositoryTableConfig<Entity>,
		eventEmitter?: EventEmitter,
	) {
		super(dynamo, config, eventEmitter);
		this.cache = new Map();
	}

	public get(key: DocumentClient.Key): Promise<IEntityResponse<Entity>> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		if (!this.cache.get(key[this.config.keySchema.hash]).has(key[this.config.keySchema.range])) {
			this.cache.get(key[this.config.keySchema.hash]).set(key[this.config.keySchema.range], super.get(key));
		}
		return this.cache.get(key[this.config.keySchema.hash]).get(key[this.config.keySchema.range]);
	}

	public async getList(keys: DocumentClient.Key[]) {
		const keysMap = new Map<DocumentClient.Key, IEntityResponse<Entity>>();
		await this.loadEntities(this.filterNotCachedKeys(keys));
		for (const key of keys) {
			keysMap.set(key, await this.getFromCache(key));
		}

		return keysMap;
	}

	public getEntityKey(e: Entity) {
		return RepositoryCached.getEntityKey(e, this.config);
	}

	public async addToCache(e: IEntityResponse<Entity>) {
		await this.addToCacheByKey(this.getEntityKey(e.entity), e);
	}

	public clear() {
		this.cache.clear();
	}

	public scan(input: IScanInput): IGenerator<Entity> {
		return new CachedRepositoryGenerator<Entity>(
			super.scan(input),
			this,
		);
	}

	public query(input: IQueryInput): IGenerator<Entity> {
		return new CachedRepositoryGenerator<Entity>(
			super.query(input),
			this,
		);
	}

	private filterNotCachedKeys(keys: DocumentClient.Key[]) {
		const notCachedKeys: DocumentClient.Key[] = [];
		for (const key of keys) {
			if (!this.keyIsCached(key)) {
				notCachedKeys.push(key);
			}
		}

		return notCachedKeys;
	}

	private async loadEntities(keys: DocumentClient.Key[]) {
		if (keys.length === 0) {
			return;
		}
		const response = await super.getList(keys);
		for (const key of keys) {
			const entity = response.get(key);
			await this.addToCacheByKey(key, entity);
		}
	}

	private keyIsCached(key: DocumentClient.Key) {
		return this.cache.has(key[this.config.keySchema.hash])
			&& this.cache.get(key[this.config.keySchema.hash]).has(key[this.config.keySchema.range]);
	}

	private getFromCache(key: DocumentClient.Key) {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			return;
		}
		return this.cache.get(key[this.config.keySchema.hash]).get(key[this.config.keySchema.range]);
	}

	private async addToCacheByKey(key: DocumentClient.Key, entityResponse: IEntityResponse<Entity>) {
		const currentCached = await this.getFromCache(key);
		if (currentCached !== undefined) {
			if (currentCached !== entityResponse.entity) {
				this.eventEmitter.emit("cacheKeyInUse", {
					cachedItem: this.getFromCache(key),
					newItem: entityResponse.entity,
				});
			}
			return;
		}
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		this.cache
			.get(key[this.config.keySchema.hash])
			.set(key[this.config.keySchema.range], Promise.resolve(entityResponse));
	}
}
