import SearchResult from "../search-result.interface";
import DynamoManagedRepository from "./repository.class";
import {DocumentClient} from "aws-sdk/clients/dynamodb";

export default class ManagedRepositoryGenerator<Entity, Marshaled extends DocumentClient.AttributeMap> implements SearchResult<Entity> {

	constructor(
		protected repository: DynamoManagedRepository<Entity, Marshaled>,
		private generator: SearchResult<Entity>,
	) {}

	public [Symbol.iterator](): SearchResult<Entity> {
		return this;
	}

	public next() {
		const next = this.generator.next();
		if (next.done) {
			return next;
		}

		return {
			done: false,
			value: new Promise<Entity>(async (rs) => {
				const entity = await next.value;
				await this.repository.track(entity);
				rs(entity);
			}),
		};
	}

	public count() {
		return this.generator.count();
	}

	public async toArray(): Promise<Entity[]> {
		const entities: Entity[] = [];
		for (const entity of this) {
			entities.push(await entity);
		}

		return entities;
	}

	public async slice(amount: number): Promise<Entity[]> {
		const entities: Entity[] = [];
		for (const entity of this) {
			entities.push(await entity);
			if (entities.length === amount) {
				break;
			}
		}

		return entities;
	}
}
