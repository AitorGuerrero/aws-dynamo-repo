import Iterable from "./iterable.interface";
import Iterator from "./iterator.interface";

export default interface SearchResult<Entity> extends Iterable<Entity>, Iterator<Entity> {
	count(): Promise<number>;
	toArray(): Promise<Entity[]>;
	slice(amount: number): Promise<Entity[]>;
}
