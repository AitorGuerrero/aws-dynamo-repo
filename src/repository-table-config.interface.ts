import {DocumentClient} from "aws-sdk/clients/dynamodb";

export enum ProjectionType {
	keysOnly = "KEYS_ONLY", include = "INCLUDE", all = "ALL",
}

export default interface RepositoryTableConfig<Entity, Marshaled extends DocumentClient.AttributeMap> {
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
