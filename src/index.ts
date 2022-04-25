import DynamoCachedRepository from "./cached/repository.class";
import DynamoManagedRepository from "./managed/repository.class";
import IQueryInput from "./query-input.interface";
import RepositoryTableConfig, * as repositoryTableConfig from "./repository-table-config.interface";
import DynamoRepository from "./repository.class";
import IDynamoRepository from "./repository.interface";
import IScanInput from "./scan-input.interface";
import ISearchResult from "./search-result.interface";

export {
	DynamoRepository,
	RepositoryTableConfig,
	repositoryTableConfig,
	DynamoCachedRepository,
	DynamoManagedRepository,
	IQueryInput,
	IScanInput,
	ISearchResult,
	IDynamoRepository,
};
