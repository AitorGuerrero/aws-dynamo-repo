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
		const entities = await this.generator.toArray();
		for (const entity of entities) {
			await this.repository.addToCache(entity);
		}

		return entities;
	}
}
