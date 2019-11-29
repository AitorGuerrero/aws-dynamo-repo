export default interface IIterator<Entity> {
	next(): {done?: boolean, value: Promise<Entity>};
}
