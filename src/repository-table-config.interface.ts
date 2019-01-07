import {DynamoDB} from "aws-sdk";

import DocumentClient = DynamoDB.DocumentClient;

export default interface IRepositoryTableConfig<Entity> {
	tableName: string;
	keySchema: {
		hash: string;
		range?: string;
	};
	secondaryIndexes?: {
		[indexName: string]: {
			ProjectionType: "KEYS_ONLY" | "INCLUDE" | "ALL";
		};
	};
	unMarshal?: (item: DocumentClient.AttributeMap) => Entity;
}
