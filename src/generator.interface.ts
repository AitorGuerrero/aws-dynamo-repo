export default interface IGenerator<Entity> {
	next(): Promise<Entity>;
	count(): Promise<number>;
	toArray(): Promise<Entity[]>;
}
