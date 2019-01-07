import {DynamoDB} from "aws-sdk";
import IScanInput from "./scan-query.interface";

import DocumentClient = DynamoDB.DocumentClient;

export default interface IQueryInput extends IScanInput {
	IndexName: DocumentClient.IndexName;
	KeyConditionExpression: DocumentClient.KeyExpression;
}