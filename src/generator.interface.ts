export default interface IEntityGenerator<Entity> {
	next(): Promise<Entity>;
	count(): Promise<number>;
	toArray(): Promise<Entity[]>;
	slice(amount: number): Promise<Entity[]>;
}
