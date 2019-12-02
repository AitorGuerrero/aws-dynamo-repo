import {DynamoDB} from "aws-sdk";
import IGenerator from "powered-dynamo/generator.interface";
import IRepositoryTableConfig from "./repository-table-config.interface";
import ISearchResult from "./search-result.interface";

export default class EntityGenerator<Entity> implements ISearchResult<Entity> {

	constructor(
		protected generator: IGenerator,
		protected tableConfig: IRepositoryTableConfig<Entity>,
		private registerVersion?: (e: Entity, v: number) => void,
	) {}

	public [Symbol.iterator](): ISearchResult<Entity> {
		return this;
	}

	public next() {
		const next = this.generator.next();
		if (next.done) {
			return next;
		}

		return {
			done: false,
			value: new Promise<Entity>(async (rs) => {
				const marshaled = await next.value;
				const entity = this.tableConfig.unMarshal(marshaled);
				if (this.tableConfig.versionKey) {
					this.registerVersion(entity, this.versionOfEntity(marshaled));
				}

				rs(entity);
			}),
		};
	}

	public count() {
		return this.generator.count();
	}

	public async toArray() {
		const result: Entity[] = [];
		for (const e of this) {
			result.push(await e);
		}

		return result;
	}

	public async slice(amount: number) {
		const result: Entity[] = [];
		for (const entityPromise of this.generator) {
			result.push(this.tableConfig.unMarshal(await entityPromise));
			if (result.length === amount) {
				break;
			}
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
