import IIterator from "../iterator.interface";
import ISearchResult from "../search-result.interface";
import DynamoCachedRepository from "./repository.class";
import {DocumentClient} from "aws-sdk/clients/dynamodb";

export default class CachedRepositoryGenerator<Entity, Marshaled extends DocumentClient.AttributeMap> implements ISearchResult<Entity> {

	constructor(
		protected repository: DynamoCachedRepository<Entity, Marshaled>,
		public generator: ISearchResult<Entity>,
	) {
	}

	public [Symbol.iterator](): IIterator<Entity> {
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
				await this.repository.addToCache(entity);
				rs(this.repository.get(this.repository.getEntityKey(entity)));
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
