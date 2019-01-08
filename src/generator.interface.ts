import IEntityResponse from "./entity-response.interface";

export default interface IGenerator<Entity> {
	next(): Promise<IEntityResponse<Entity>>;
	count(): Promise<number>;
	toArray(): Promise<Array<IEntityResponse<Entity>>>;
}
