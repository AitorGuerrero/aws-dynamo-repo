import Iterator from "./iterator.interface";

export default interface Iterable<Entity> {
	[Symbol.iterator](): Iterator<Entity>;
}
