import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import PoweredDynamo from "powered-dynamo";
import DynamoRepository, {QueryInput, ScanInput} from '../repository.class';

import DocumentClient = DynamoDB.DocumentClient;
import {TableConfig} from './table-config';

export default class Repository<Entity> extends DynamoRepository<Entity> {

	protected static getEntityKey<Entity>(entity: Entity, tableConfig: TableConfig<unknown>): DocumentClient.Key {
		const marshaledEntity = tableConfig.marshal!(entity);
		const key: DocumentClient.Key = {};
		key[tableConfig.keySchema.hash] = marshaledEntity[tableConfig.keySchema.hash];
		if (tableConfig.keySchema.range) {
			key[tableConfig.keySchema.range] = marshaledEntity[tableConfig.keySchema.range];
		}

		return key;
	}

	protected readonly cache: Map<unknown, Map<unknown, Promise<Entity | undefined>>>;

	constructor(
		protected dynamo: PoweredDynamo,
		config: TableConfig<Entity>,
		public readonly eventEmitter: EventEmitter = new EventEmitter(),
	) {
		super(dynamo, config);
		this.cache = new Map();
	}

	public get(key: DocumentClient.Key): Promise<Entity | undefined> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		if (!this.cache.get(key[this.config.keySchema.hash])!.has(key[this.config.keySchema.range as any])) {
			this.cache.get(key[this.config.keySchema.hash])!.set(key[this.config.keySchema.range as any], super.get(key));
		}
		return this.cache.get(key[this.config.keySchema.hash])!.get(key[this.config.keySchema.range as any])!;
	}

	public async getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity | undefined>> {
		const keysMap = new Map<DocumentClient.Key, Entity | undefined>();
		await this.loadEntities(this.filterNotCachedKeys(keys));
		for (const key of keys) {
			keysMap.set(key, await this.getFromCache(key));
		}

		return keysMap;
	}

	public getEntityKey(e: Entity): DocumentClient.Key {
		return Repository.getEntityKey(e, this.config);
	}

	public async addToCache(e: Entity): Promise<void> {
		await this.addToCacheByKey(this.getEntityKey(e), e);
	}

	public clear(): void {
		this.cache.clear();
	}

	public async* scan(input: ScanInput): AsyncGenerator<Entity> {
		for await (const entity of super.scan(input)) {
			await this.addToCache(entity);
			yield (await this.get(this.getEntityKey(entity))) as Entity;
		}
	}

	public async* query(input: QueryInput): AsyncGenerator<Entity> {
		for await (const entity of super.query(input)) {
			await this.addToCache(entity);
			yield (await this.get(this.getEntityKey(entity))) as Entity;
		}
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
			&& this.cache.get(key[this.config.keySchema.hash])!.has(key[this.config.keySchema.range as any]);
	}

	private async getFromCache(key: DocumentClient.Key): Promise<Entity | undefined> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			return;
		}
		return this.cache.get(key[this.config.keySchema.hash])!.get(key[this.config.keySchema.range as any]);
	}

	private async addToCacheByKey(key: DocumentClient.Key, entity: Entity | undefined): Promise<void> {
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
		this.cache.get(key[this.config.keySchema.hash])!.set(key[this.config.keySchema.range as any], Promise.resolve(entity) as any);
	}
}
