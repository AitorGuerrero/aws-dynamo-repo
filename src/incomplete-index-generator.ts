import {DynamoDB} from "aws-sdk";
import IGenerator from "powered-dynamo/generator.interface";
import EntityGenerator from "./generator.class";
import RepositoryTableConfig from "./repository-table-config.interface";
import DynamoRepository from "./repository.class";
import {DocumentClient} from "aws-sdk/clients/dynamodb";

export default class IncompleteIndexGenerator<Entity, Marshaled extends DocumentClient.AttributeMap> extends EntityGenerator<Entity, Marshaled> {

	constructor(
		private repository: DynamoRepository<Entity, Marshaled>,
		generator: IGenerator,
		tableConfig: RepositoryTableConfig<Entity, Marshaled>,
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

				rs(this.repository.get(key));
			}),
		});
	}
}
