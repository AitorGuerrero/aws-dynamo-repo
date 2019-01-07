import {DynamoDB} from "aws-sdk";
import QueryGenerator from "powered-dynamo/query-generator.class";
import RepositoryGenerator from "./generator.class";

export default class RepositoryQueryGenerator<Entity> extends RepositoryGenerator<Entity> {

	constructor(
		private scanGenerator: QueryGenerator,
		private unMarshal: (item: DynamoDB.DocumentClient.AttributeMap) => Entity,
	) {
		super();
	}

	public async next(): Promise<Entity> {
		const next = await this.scanGenerator.next();
		if (next === undefined) {
			return;
		}

		return this.unMarshal(next);
	}

	public count() {
		return this.scanGenerator.count();
	}
}
