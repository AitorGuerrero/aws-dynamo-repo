import IIterable from "./iterable.interface";
import Iterator from "./iterator.interface";

export default interface ISearchResult<Entity> extends IIterable<Entity>, Iterator<Entity> {
	count(): Promise<number>;
	toArray(): Promise<Entity[]>;
	slice(amount: number): Promise<Entity[]>;
}
