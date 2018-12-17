import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import DocumentClient = DynamoDB.DocumentClient;
import generatorToArray from "./generator-to-array";
import {DynamoDBRepository, IGenerator, IRepositoryTableConfig, ISearchInput} from "./repository.class";

export class RepositoryCached<Entity> extends DynamoDBRepository<Entity> {

	private readonly cache: Map<any, Map<any, Promise<Entity>>>;

	constructor(
		dc: DocumentClient,
		config: IRepositoryTableConfig<Entity>,
		eventEmitter?: EventEmitter,
	) {
		super(dc, config, eventEmitter);
		this.cache = new Map();
	}

	public get(key: DocumentClient.Key): Promise<Entity> {
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		if (!this.cache.get(key[this.config.keySchema.hash]).has(key[this.config.keySchema.range])) {
			this.cache.get(key[this.config.keySchema.hash]).set(key[this.config.keySchema.range], super.get(key));
		}
		return this.cache.get(key[this.config.keySchema.hash]).get(key[this.config.keySchema.range]);
	}

	public async getList(keys: DocumentClient.Key[]) {
		const keysMap = new Map<DocumentClient.Key, Entity>();
		await this.loadEntities(this.filterNotCachedKeys(keys));
		for (const key of keys) {
			keysMap.set(key, await this.getFromCache(key));
		}

		return keysMap;
	}

	public search(input: ISearchInput) {
		const getNextEntity = super.search(input);
		const generator = (async () => {
			const entity = await getNextEntity();
			if (entity === undefined) {
				return;
			}
			const key = this.getEntityKey(entity);
			await this.addToCacheByKey(key, entity);

			return this.getFromCache(key);
		}) as IGenerator<Entity>;
		generator.toArray = generatorToArray;

		return generator;
	}

	public getEntityKey(e: Entity) {
		return DynamoDBRepository.getEntityKey<Entity>(e, this.config);
	}

	public async addToCache(e: Entity) {
		await this.addToCacheByKey(this.getEntityKey(e), e);
	}

	public clear() {
		this.cache.clear();
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

	private async addToCacheByKey(key: DocumentClient.Key, entity: Entity) {
		const currentCached = await this.getFromCache(key);
		if (currentCached !== undefined) {
			if (currentCached !== entity) {
				this.eventEmitter.emit("cacheKeyInUse", {
					cachedItem: this.getFromCache(key),
					newItem: entity,
				});
			}
			return;
		}
		if (!this.cache.has(key[this.config.keySchema.hash])) {
			this.cache.set(key[this.config.keySchema.hash], new Map());
		}
		this.cache.get(key[this.config.keySchema.hash]).set(key[this.config.keySchema.range], Promise.resolve(entity));
	}
}
