import RepositoryCached from "./src/cached/repository.class";
import RepositoryManaged from "./src/managed/repository.class";
import IRepositoryTableConfig, * as repositoryTableConfig from "./src/repository-table-config.interface";
import DynamoDBRepository from "./src/repository.class";

export {
	DynamoDBRepository,
	IRepositoryTableConfig,
	repositoryTableConfig,
	RepositoryCached,
	RepositoryManaged,
};
