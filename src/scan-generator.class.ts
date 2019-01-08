import {DynamoDB} from "aws-sdk";
import ScanGenerator from "powered-dynamo/scan-generator.class";
import IEntityResponse from "./entity-response.interface";
import RepositoryGenerator from "./generator.class";

export default class RepositoryScanGenerator<Entity> extends RepositoryGenerator<Entity> {

	constructor(
		private scanGenerator: ScanGenerator,
		private unMarshal: (item: DynamoDB.DocumentClient.AttributeMap) => Entity,
		private versionKey?: string,
	) {
		super();
	}

	public async next(): Promise<IEntityResponse<Entity>> {
		const next = await this.scanGenerator.next();
		if (next === undefined) {
			return;
		}

		return {entity: this.unMarshal(next), version: this.versionKey ? next[this.versionKey] : undefined};
	}

	public count() {
		return this.scanGenerator.count();
	}
}
