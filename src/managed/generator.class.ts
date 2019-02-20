import IGenerator from "powered-dynamo/generator.interface";
import CachedEntityGenerator from "../cached/generator.class";
import IRepositoryTableConfig from "../repository-table-config.interface";
import RepositoryManaged from "./repository.class";

export default class ManagedRepositoryGenerator<Entity> extends CachedEntityGenerator<Entity> {

	constructor(
		protected repository: RepositoryManaged<Entity>,
		generator: IGenerator,
		tableConfig: IRepositoryTableConfig<Entity>,
		registerVersion: (e: Entity, v: number) => void,
	) {
		super(repository, generator, tableConfig, registerVersion);
	}

	public async next() {
		const response = await super.next();
		if (response === undefined) {
			return;
		}
		await this.repository.track(response);

		return response;
	}

	public count() {
		return this.generator.count();
	}
}
