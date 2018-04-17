import {DynamoDB} from "aws-sdk";
import DynamoEntityManager from "./entity-manager.class";
import {IDynamoDBRepository, ISearchInput} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

export class RepositoryManaged<Entity> implements IDynamoDBRepository<Entity> {
	constructor(
		private tableName: string,
		private repository: IDynamoDBRepository<Entity>,
		private entityManager: DynamoEntityManager,
	) {

	}

	public async get(key: DocumentClient.Key) {
		const entity = await this.repository.get(key);
		this.entityManager.track(this.tableName, entity);

		return entity;
	}

	public async getList(keys: DocumentClient.Key[]) {
		const list = await this.repository.getList(keys);
		for (const entity of list.values()) {
			this.entityManager.track(this.tableName, entity);
		}

		return list;
	}

	public search(input: ISearchInput) {
		const generator = this.repository.search(input);

		return async () => {
			const entity = await generator();
			this.entityManager.track(this.tableName, entity);

			return entity;
		};
	}

	public async count(input: ISearchInput) {
		return this.repository.count(input);
	}
}
