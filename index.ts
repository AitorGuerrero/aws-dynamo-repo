import DynamoCachedRepository from "./src/cached/repository.class";
import DynamoManagedRepository from "./src/managed/repository.class";
import IQueryInput from "./src/query-input.interface";
import IRepositoryTableConfig, * as repositoryTableConfig from "./src/repository-table-config.interface";
import DynamoRepository from "./src/repository.class";
import IDynamoRepository from "./src/repository.interface";
import IScanInput from "./src/scan-input.interface";
import ISearchResult from "./src/search-result.interface";

export {
	DynamoRepository,
	IRepositoryTableConfig,
	repositoryTableConfig,
	DynamoCachedRepository,
	DynamoManagedRepository,
	IQueryInput,
	IScanInput,
	ISearchResult,
	IDynamoRepository,
};
