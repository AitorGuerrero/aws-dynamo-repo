export default interface Iterator<Entity> {
	next(): {done?: boolean, value: Promise<Entity>};
}
