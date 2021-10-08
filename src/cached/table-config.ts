import RepositoryTableConfig from '../repository-table-config.interface';
import {DynamoDB} from 'aws-sdk';

export interface TableConfig<Entity> extends RepositoryTableConfig<Entity> {
	marshal?: (e: Entity) => DynamoDB.DocumentClient.AttributeMap;
}
