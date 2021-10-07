import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import RepositoryTableConfig from "../repository-table-config.interface";
import DynamoRepository, {QueryInput, ScanInput} from '../repository.class';
import SearchResult from "../search-result.interface";
import CachedRepositoryGenerator from "./generator.class";

import DocumentClient = DynamoDB.DocumentClient;

export interface CachedRepositoryTableConfig<Entity> extends RepositoryTableConfig<Entity> {
	marshal?: (e: Entity) => DocumentClient.AttributeMap;
}

export default class DynamoCachedRepository<Entity> extends DynamoRepository<Entity> {

	protected static getEntityKey<Entity>(entity: Entity, tableConfig: CachedRepositoryTableConfig<unknown>): DocumentClient.Key {
		const marshaledEntity = tableConfig.marshal(entity);
		const key: DocumentClient.Key = {};
		key[tableConfig.keySchema.hash] = marshaledEntity[tableConfig.keySchema.hash];
		if (tableConfig.keySchema.range) {
			key[tableConfig.keySchema.range] = marshaledEntity[tableConfig.keySchema.range];
		}

		return key;
	}

	protected readonly cache: Map<any, Map<any, Promise<Entity>>>;

	constructor(
		protected dynamo: PoweredDynamo,
		config: CachedRepositoryTableConfig<Entity>,
		public readonly eventEmitter: EventEmitter = new EventEmitter(),
	) {
		super(dynamo, config);
		this.cache = new Map();
	}

	public get(key: DocumentClient.Key): Promise<Entity | undefined> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		if (!this.cache.get(key[this.config.keySchema.hash]).has(key[this.config.keySchema.range])) {
			this.cache.get(key[this.config.keySchema.hash]).set(key[this.config.keySchema.range], super.get(key));
		}
		return this.cache.get(key[this.config.keySchema.hash]).get(key[this.config.keySchema.range]);
	}

	public async getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity | undefined>> {
		const keysMap = new Map<DocumentClient.Key, Entity>();
		await this.loadEntities(this.filterNotCachedKeys(keys));
		for (const key of keys) {
			keysMap.set(key, await this.getFromCache(key));
		}

		return keysMap;
	}

	public getEntityKey(e: Entity): DocumentClient.Key {
		return DynamoCachedRepository.getEntityKey(e, this.config);
	}

	public async addToCache(e: Entity): Promise<void> {
		await this.addToCacheByKey(this.getEntityKey(e), e);
	}

	public clear(): void {
		this.cache.clear();
	}

	public async scan(input: ScanInput): Promise<SearchResult<Entity>> {
		return new CachedRepositoryGenerator<Entity>(
			this,
			await super.scan(input),
		);
	}

	public async query(input: QueryInput): Promise<SearchResult<Entity>> {
		return new CachedRepositoryGenerator<Entity>(
			this,
			await super.query(input),
		);
	}

	private filterNotCachedKeys(keys: DocumentClient.Key[]): DocumentClient.Key[] {
		const notCachedKeys: DocumentClient.Key[] = [];
		for (const key of keys) {
			if (!this.keyIsCached(key)) {
				notCachedKeys.push(key);
			}
		}

		return notCachedKeys;
	}

	private async loadEntities(keys: DocumentClient.Key[]): Promise<void> {
		if (keys.length === 0) {
			return;
		}
		const response = await super.getList(keys);
		for (const key of keys) {
			const entityResponse = response.get(key);
			await this.addToCacheByKey(key, entityResponse);
		}
	}

	private keyIsCached(key: DocumentClient.Key): boolean {
		return this.cache.has(key[this.config.keySchema.hash])
			&& this.cache.get(key[this.config.keySchema.hash]).has(key[this.config.keySchema.range]);
	}

	private getFromCache(key: DocumentClient.Key): Promise<Entity | undefined> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			return;
		}
		return this.cache.get(key[this.config.keySchema.hash]).get(key[this.config.keySchema.range]);
	}

	private async addToCacheByKey(key: DocumentClient.Key, entity: Entity): Promise<void> {
		const currentCached = await this.getFromCache(key);
		if (currentCached !== undefined) {
			if (currentCached !== entity) {
				this.eventEmitter.emit("cacheKeyInUse", {
					cachedItem: currentCached,
					newItem: entity,
				});
			}
			return;
		}
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		this.cache
			.get(key[this.config.keySchema.hash])
			.set(key[this.config.keySchema.range], Promise.resolve(entity));
	}
}
