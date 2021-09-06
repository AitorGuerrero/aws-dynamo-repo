import RepositoryTableConfig from "./repository-table-config.interface";
import { ProjectionType } from "./repository-table-config.interface";
import DynamoRepository, {QueryInput, ScanInput } from "./repository.class";
import SearchResult from "./search-result.interface";

export {
	DynamoRepository,
	RepositoryTableConfig,
	ProjectionType,
	SearchResult,
	ScanInput,
	QueryInput,
};
