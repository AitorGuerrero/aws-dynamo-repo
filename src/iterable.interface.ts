import IIterator from "./iterator.interface";

export default interface IIterable<Entity> {
	[Symbol.iterator](): IIterator<Entity>;
}
