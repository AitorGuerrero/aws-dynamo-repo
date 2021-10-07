import {DynamoDB} from "aws-sdk";
import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "./generator.class";
import RepositoryTableConfig from "./repository-table-config.interface";
import DynamoRepository from "./repository.class";

export default class IncompleteIndexGenerator<Entity> extends EntityGenerator<Entity> {

	constructor(
		private repository: DynamoRepository<Entity>,
		generator: IGenerator,
		tableConfig: RepositoryTableConfig<Entity>,
		registerVersion?: (entity: Entity, v: number) => void,
	) {
		super(generator, tableConfig, registerVersion);
	}

	public next() {
		const next = this.generator.next();
		if (next.done) {
			return next as any;
		}

		return Object.assign({}, next, {
			value: new Promise<Entity>(async (rs) => {
				const entity = await next.value;
				const key: DynamoDB.DocumentClient.Key = {
					[this.tableConfig.keySchema.hash]: entity[this.tableConfig.keySchema.hash],
				};
				if (this.tableConfig.keySchema.range) {
					key[this.tableConfig.keySchema.range] = entity[this.tableConfig.keySchema.range];
				}

				rs((await this.repository.get(key)) as Entity);
			}),
		});
	}
}
