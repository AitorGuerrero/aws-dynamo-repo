import {DynamoDB} from "aws-sdk";
import IQueryInput from "./query-input.interface";
import IScanInput from "./scan-input.interface";
import ISearchResult from "./search-result.interface";

import DocumentClient = DynamoDB.DocumentClient;

export default interface IDynamoRepository<Entity> {
	get(Key: DocumentClient.Key): Promise<Entity>;
	getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity>>;
	scan(input: IScanInput): Promise<ISearchResult<Entity>>;
	query(input: IQueryInput): Promise<ISearchResult<Entity>>;
}
