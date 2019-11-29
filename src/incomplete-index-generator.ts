import {DynamoDB} from "aws-sdk";
import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "./generator.class";
import IRepositoryTableConfig from "./repository-table-config.interface";
import DynamoRepository from "./repository.class";

export default class IncompleteIndexGenerator<E> extends EntityGenerator<E> {

	constructor(
		private repository: DynamoRepository<E>,
		generator: IGenerator,
		tableConfig: IRepositoryTableConfig<E>,
		registerVersion?: (entity: E, v: number) => void,
	) {
		super(generator, tableConfig, registerVersion);
	}

	public next() {
		const next = this.generator.next();
		if (next.done) {
			return next;
		}

		return Object.assign({}, next, {
			value: new Promise<E>(async (rs) => {
				const entity = await next.value;
				const key: DynamoDB.DocumentClient.Key = {
					[this.tableConfig.keySchema.hash]: entity[this.tableConfig.keySchema.hash],
				};
				if (this.tableConfig.keySchema.range) {
					key[this.tableConfig.keySchema.range] = entity[this.tableConfig.keySchema.range];
				}

				rs(this.repository.get(key));
			}),
		});
	}
}
