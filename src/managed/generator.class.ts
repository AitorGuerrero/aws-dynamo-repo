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
		const response = await this.generator.next();
		if (response.entity === undefined) {
			return {};
		}
		await this.repository.track(response.entity, response.version);

		return response;
	}

	public count() {
		return this.generator.count();
	}
}
