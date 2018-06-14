import {DynamoDB} from "aws-sdk";
import DynamoEntityManager, {IEntityManagerTableConfig} from "./entity-manager.class";
import generatorToArray from "./generator-to-array";
import {RepositoryCached} from "./repository.cached.class";
import {IGenerator, ISearchInput} from "./repository.class";

export class RepositoryManaged<Entity> extends RepositoryCached<Entity> {

	constructor(
		private entityName: string,
		tableConfig: IEntityManagerTableConfig<Entity>,
		documentClient: DynamoDB.DocumentClient,
		private entityManager: DynamoEntityManager,
	) {
		super(documentClient, tableConfig);
		this.entityManager.addTableConfig(entityName, tableConfig);
	}

	public async get(key: DynamoDB.DocumentClient.Key) {
		const entity = await super.get(key);
		this.entityManager.track(this.entityName, entity);

		return entity;
	}

	public async getList(keys: DynamoDB.DocumentClient.Key[]) {
		const list = await super.getList(keys);
		for (const entity of list.values()) {
			this.entityManager.track(this.entityName, entity);
		}

		return list;
	}

	public search(input: ISearchInput) {
		const generator = super.search(input);
		const managedGenerator = (async () => {
			const entity = await generator();
			this.entityManager.track(this.entityName, entity);

			return entity;
		}) as IGenerator<Entity>;
		managedGenerator.toArray = generatorToArray;

		return managedGenerator;
	}

	public async persist(e: Entity) {
		this.entityManager.add(this.entityName, e);
	}
}
