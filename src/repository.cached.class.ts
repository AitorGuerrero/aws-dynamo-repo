import {DynamoDB} from "aws-sdk";
import DocumentClient = DynamoDB.DocumentClient;
import generatorToArray from "./generator-to-array";
import getEntityKey from "./get-entity-key";
import {IGenerator, ISearchInput} from "./repository.class";
import IDynamoDBRepository from "./repository.interface";

export class RepositoryCached<Entity> implements IDynamoDBRepository<Entity> {

	private readonly cache: Map<any, Map<any, Promise<Entity>>>;
	private readonly hashKey: string;
	private readonly rangeKey: string;

	constructor(
		private repo: IDynamoDBRepository<Entity>,
		private keySchema: DocumentClient.KeySchema,
	) {
		this.cache = new Map();
		this.hashKey = keySchema.find((k) => k.KeyType === "HASH").AttributeName;
		const rangeSchema = keySchema.find((k) => k.KeyType === "RANGE");
		this.rangeKey = rangeSchema ? rangeSchema.AttributeName : undefined;
	}

	public get(key: DocumentClient.Key): Promise<Entity> {
		if (!this.cache.has(key[this.hashKey])) {
			this.cache.set(key[this.hashKey], new Map());
		}
		if (!this.cache.get(key[this.hashKey]).has(key[this.rangeKey])) {
			this.cache.get(key[this.hashKey]).set(key[this.rangeKey], this.repo.get(key));
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
			const response = await this.repo.getList(notCachedKeys);
			for (const key of notCachedKeys) {
				const entity = response.get(key);
				this.addToCacheByKey(key, entity);
				result.set(key, await this.getFromCache(key));
			}
		}
		for (const key of keys) {
			result.set(key, await this.getFromCache(key));
		}

		return result;
	}

	public search(input: ISearchInput) {
		const getNextEntity = this.repo.search(input);
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

	public count(input: ISearchInput) {
		return this.repo.count(input);
	}

	public getEntityKey(e: Entity) {
		return getEntityKey(this.keySchema, e);
	}

	public addToCache(e: Entity) {
		this.addToCacheByKey(this.getEntityKey(e), e);
	}

	public async persist(e: Entity) {
		this.addToCache(e);
		await this.repo.persist(e);
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

	private addToCacheByKey(key: DocumentClient.Key, entity: Entity) {
		if (this.keyIsCached(key)) {
			return;
		}
		if (!this.cache.has(key[this.hashKey])) {
			this.cache.set(key[this.hashKey], new Map());
		}
		this.cache.get(key[this.hashKey]).set(key[this.rangeKey], Promise.resolve(entity));
	}
}
