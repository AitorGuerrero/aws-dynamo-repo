import ISearchResult from "../search-result.interface";
import DynamoManagedRepository from "./repository.class";

export default class ManagedRepositoryGenerator<Entity> implements ISearchResult<Entity> {

	constructor(
		protected repository: DynamoManagedRepository<Entity>,
		private generator: ISearchResult<Entity>,
	) {}

	public [Symbol.iterator](): ISearchResult<Entity> {
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
