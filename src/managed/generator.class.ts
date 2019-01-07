import RepositoryGenerator from "../generator.class";
import IGenerator from "../generator.interface";
import RepositoryManaged from "./repository.class";

export default class ManagedRepositoryGenerator<Entity> extends RepositoryGenerator<Entity> {
	constructor(
		protected generator: IGenerator<Entity>,
		protected repository: RepositoryManaged<Entity>,
	) {
		super();
	}

	public async next() {
		const entity = await this.generator.next();
		if (entity === undefined) {
			return;
		}
		await this.repository.persist(entity);

		return entity;
	}

	public count() {
		return this.generator.count();
	}
}
