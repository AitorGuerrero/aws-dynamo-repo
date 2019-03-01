import {DynamoDB} from "aws-sdk";
import IGenerator from "powered-dynamo/generator.interface";
import IEntityGenerator from "./generator.interface";
import IRepositoryTableConfig from "./repository-table-config.interface";

export default class EntityGenerator<Entity> implements IEntityGenerator<Entity> {

	constructor(
		protected generator: IGenerator,
		protected tableConfig: IRepositoryTableConfig<Entity>,
		private registerVersion?: (e: Entity, v: number) => void,
	) {}

	public async next(): Promise<Entity> {
		const next = await this.generator.next();
		if (next === undefined) {
			return;
		}
		const entity = this.tableConfig.unMarshal(next);
		if (this.tableConfig.versionKey) {
			this.registerVersion(entity, this.versionOfEntity(next));
		}

		return entity;
	}

	public count() {
		return this.generator.count();
	}

	public async toArray() {
		let e: Entity;
		const result: Entity[] = [];
		while (e = await this.next()) {
			result.push(e);
		}

		return result;
	}

	private versionOfEntity(item: DynamoDB.DocumentClient.AttributeMap) {
		if (this.tableConfig.versionKey === undefined) {
			return undefined;
		}

		return item[this.tableConfig.versionKey];
	}
}
