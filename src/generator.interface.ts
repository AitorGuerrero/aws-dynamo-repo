export default interface IEntityGenerator<Entity> {
	next(): Promise<Entity>;
	count(): Promise<number>;
	toArray(): Promise<Entity[]>;
}
