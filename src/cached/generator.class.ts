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
		const result = await this.generator.next();
		if (result === undefined) {
			return;
		}
		await this.repository.addToCache(result.entity);

		return result;
	}

	public count() {
		return this.generator.count();
	}
}
