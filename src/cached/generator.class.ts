import Iterator from "../iterator.interface";
import SearchResult from "../search-result.interface";
import DynamoCachedRepository from "./repository.class";

export default class CachedRepositoryGenerator<Entity> implements SearchResult<Entity> {

	constructor(
		protected repository: DynamoCachedRepository<Entity>,
		public generator: SearchResult<Entity>,
	) {
	}

	public [Symbol.iterator](): Iterator<Entity> {
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
				const key = this.repository.getEntityKey(entity);
				rs((await this.repository.get(key)) as Entity);
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
