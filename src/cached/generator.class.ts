import RepositoryGenerator from "../generator.class";
import IGenerator from "../generator.interface";
import RepositoryCached from "./repository.class";

export default class CachedRepositoryGenerator<Entity> extends RepositoryGenerator<Entity> {

	constructor(
		protected generator: IGenerator<Entity>,
		protected repository: RepositoryCached<Entity>,
	) {
		super();
	}

	public async next() {
		const entity = await this.generator.next();
		if (entity === undefined) {
			return;
		}
		await this.repository.addToCache(entity);

		return entity;
	}

	public count() {
		return this.generator.count();
	}
}
