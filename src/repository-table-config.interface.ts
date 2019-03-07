import {DynamoDB} from "aws-sdk";

import DocumentClient = DynamoDB.DocumentClient;

export enum ProjectionType {
	keysOnly = "KEYS_ONLY", include = "INCLUDE", all = "ALL",
}

export default interface IRepositoryTableConfig<Entity> {
	tableName: string;
	keySchema: {
		hash: string;
		range?: string;
	};
	secondaryIndexes?: {
		[indexName: string]: {
			ProjectionType: ProjectionType;
		};
	};
	versionKey?: string;
	unMarshal?: (item: DocumentClient.AttributeMap) => Entity;
}
