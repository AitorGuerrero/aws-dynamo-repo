import {DynamoDB} from "aws-sdk";
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
		if (next.done) {
			return;
		}
		const entity = await next.value;
		const key: DynamoDB.DocumentClient.Key = {
			[this.tableConfig.keySchema.hash]: entity[this.tableConfig.keySchema.hash],
		};
		if (this.tableConfig.keySchema.range) {
			key[this.tableConfig.keySchema.range] = entity[this.tableConfig.keySchema.range];
		}

		return await this.repository.get(key);
	}
}
