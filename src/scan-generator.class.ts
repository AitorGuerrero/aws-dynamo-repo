import {DynamoDB} from "aws-sdk";
import ScanGenerator from "powered-dynamo/scan-generator.class";
import RepositoryGenerator from "./generator.class";

export default class RepositoryScanGenerator<Entity> extends RepositoryGenerator<Entity> {

	constructor(
		private scanGenerator: ScanGenerator,
		private unMarshal: (item: DynamoDB.DocumentClient.AttributeMap) => Entity,
	) {
		super();
	}

	public async next() {
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
