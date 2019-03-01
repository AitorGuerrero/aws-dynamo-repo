import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "./generator.class";
import IRepositoryTableConfig from "./repository-table-config.interface";
import DynamoDBRepository from "./repository.class";

export default class IncompleteIndexGenerator<E> extends EntityGenerator<E> {

	constructor(
		private repository: DynamoDBRepository<E>,
		generator: IGenerator,
		tableConfig: IRepositoryTableConfig<E>,
		registerVersion?: (entity: E, v: number) => void,
	) {
		super(generator, tableConfig, registerVersion);
	}

	public async next() {
		const next = await this.generator.next();
		if (next === undefined) {
			return;
		}

		return await this.repository.get(next);
	}
}
