import {DynamoDB} from "aws-sdk";
import {EntityGenerator, ISearchInput} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

export default interface IDynamoDBRepository<Entity> {
	get(key: DocumentClient.Key): Promise<Entity>;
	getList(keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, Entity>>;
	search(input: ISearchInput): EntityGenerator<Entity>;
	count(input: ISearchInput): Promise<number>;
	persist(e: Entity): Promise<any>;
}
