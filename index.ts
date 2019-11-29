import DynamoCachedRepository from "./src/cached/repository.class";
import DynamoManagedRepository from "./src/managed/repository.class";
import IRepositoryTableConfig, * as repositoryTableConfig from "./src/repository-table-config.interface";
import DynamoRepository from "./src/repository.class";

export {
	DynamoRepository,
	IRepositoryTableConfig,
	repositoryTableConfig,
	DynamoCachedRepository,
	DynamoManagedRepository,
};
