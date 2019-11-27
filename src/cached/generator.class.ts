import IEntityGenerator from "../generator.interface";
import RepositoryCached from "./repository.class";

export default class CachedRepositoryGenerator<Entity> implements IEntityGenerator<Entity> {

	constructor(
		protected repository: RepositoryCached<Entity>,
		private generator: IEntityGenerator<Entity>,
	) {
	}

	public async next() {
		const entity = await this.generator.next();
		if (entity === undefined) {
			return;
		}
		await this.repository.addToCache(entity);

		return this.repository.get(this.repository.getEntityKey(entity));
	}

	public count() {
		return this.generator.count();
	}

	public async toArray(): Promise<Entity[]> {
		const entities: Entity[] = [];
		let entity: Entity;
		while (entity = await this.next()) {
			entities.push(entity);
		}

		return entities;
	}

	public async slice(amount: number): Promise<Entity[]> {
		const items = await this.generator.slice(amount);
		const cachedItems: Entity[] = [];
		for (const item of items) {
			await this.repository.addToCache(item);
			cachedItems.push(await this.repository.get(this.repository.getEntityKey(item)));
		}

		return cachedItems;
	}
}
