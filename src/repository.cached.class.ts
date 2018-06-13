import {DynamoDB} from "aws-sdk";
import DocumentClient = DynamoDB.DocumentClient;
import generatorToArray from "./generator-to-array";
import getEntityKey from "./get-entity-key";
import {DynamoDBRepository, IGenerator, IRepositoryTableConfig, ISearchInput} from "./repository.class";

export class RepositoryCached<Entity> extends DynamoDBRepository<Entity> {

	private readonly cache: Map<any, Map<any, Promise<Entity>>>;
	private readonly hashKey: string;
	private readonly rangeKey: string;

	constructor(
		dc: DocumentClient,
		config: IRepositoryTableConfig<Entity>,
	) {
		super(dc, config);
		this.cache = new Map();
		this.hashKey = this.config.keySchema.find((k) => k.KeyType === "HASH").AttributeName;
		const rangeSchema = this.config.keySchema.find((k) => k.KeyType === "RANGE");
		this.rangeKey = rangeSchema ? rangeSchema.AttributeName : undefined;
	}

	public get(key: DocumentClient.Key): Promise<Entity> {
		if (!this.cache.has(key[this.hashKey])) {
			this.cache.set(key[this.hashKey], new Map());
		}
		if (!this.cache.get(key[this.hashKey]).has(key[this.rangeKey])) {
			this.cache.get(key[this.hashKey]).set(key[this.rangeKey], super.get(key));
		}
		return this.cache.get(key[this.hashKey]).get(key[this.rangeKey]);
	}

	public async getList(keys: DocumentClient.Key[]) {
		const result = new Map<DocumentClient.Key, Entity>();
		const notCachedKeys: DocumentClient.Key[] = [];
		for (const key of keys) {
			if (!this.keyIsCached(key)) {
				notCachedKeys.push(key);
			}
		}
		if (notCachedKeys.length > 0) {
			const response = await super.getList(notCachedKeys);
			for (const key of notCachedKeys) {
				const entity = response.get(key);
				await this.addToCacheByKey(key, entity);
				result.set(key, await this.getFromCache(key));
			}
		}
		for (const key of keys) {
			result.set(key, await this.getFromCache(key));
		}

		return result;
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
		return getEntityKey(this.config.keySchema, this.config.marshal(e));
	}

	public async addToCache(e: Entity) {
		await this.addToCacheByKey(this.getEntityKey(e), e);
	}

	public clear() {
		this.cache.clear();
	}

	private keyIsCached(key: DocumentClient.Key) {
		return this.cache.has(key[this.hashKey]) && this.cache.get(key[this.hashKey]).has(key[this.rangeKey]);
	}

	private getFromCache(key: DocumentClient.Key) {
		if (!this.cache.has(key[this.hashKey])) {
			return;
		}
		return this.cache.get(key[this.hashKey]).get(key[this.rangeKey]);
	}

	private async addToCacheByKey(key: DocumentClient.Key, entity: Entity) {
		const currentCached = await this.getFromCache(key);
		if (currentCached !== undefined) {
			if (currentCached !== entity) {
				throw new Error("Trying to add to cache a key in use");
			} else {
				return;
			}
		}
		if (!this.cache.has(key[this.hashKey])) {
			this.cache.set(key[this.hashKey], new Map());
		}
		this.cache.get(key[this.hashKey]).set(key[this.rangeKey], Promise.resolve(entity));
	}
}
