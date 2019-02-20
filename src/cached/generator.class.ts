import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "../generator.class";
import IRepositoryTableConfig from "../repository-table-config.interface";
import RepositoryCached from "./repository.class";

export default class CachedRepositoryGenerator<Entity> extends EntityGenerator<Entity> {

	constructor(
		protected repository: RepositoryCached<Entity>,
		generator: IGenerator,
		tableConfig: IRepositoryTableConfig<Entity>,
		registerVersion: (e: Entity, v: number) => void,
	) {
		super(generator, tableConfig, registerVersion);
	}

	public async next() {
		const entity = await super.next();
		if (entity === undefined) {
			return;
		}
		await this.repository.addToCache(entity);

		return this.repository.get(this.repository.getEntityKey(entity));
	}

	public count() {
		return this.generator.count();
	}
}
