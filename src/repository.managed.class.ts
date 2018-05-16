import {DynamoDB} from "aws-sdk";
import DynamoEntityManager, {ITableConfig} from "./entity-manager.class";
import generatorToArray from "./generator-to-array";
import {IDynamoDBRepository, IGenerator, ISearchInput} from "./repository.class";

export class RepositoryManaged<Entity> implements IDynamoDBRepository<Entity> {

	constructor(
		private entityName: string,
		private tableConfig: ITableConfig<Entity>,
		private repository: IDynamoDBRepository<Entity>,
		private entityManager: DynamoEntityManager,
	) {
		this.entityManager.addTableConfig(entityName, tableConfig);
	}

	public async get(key: DynamoDB.DocumentClient.Key) {
		const entity = await this.repository.get(key);
		this.entityManager.track(this.entityName, entity);

		return entity;
	}

	public async getList(keys: DynamoDB.DocumentClient.Key[]) {
		const list = await this.repository.getList(keys);
		for (const entity of list.values()) {
			this.entityManager.track(this.entityName, entity);
		}

		return list;
	}

	public search(input: ISearchInput) {
		const generator = this.repository.search(input);
		const managedGenerator = (async () => {
			const entity = await generator();
			this.entityManager.track(this.entityName, entity);

			return entity;
		}) as IGenerator<Entity>;
		managedGenerator.toArray = generatorToArray;

		return managedGenerator;
	}

	public async count(input: ISearchInput) {
		return this.repository.count(input);
	}
}
