import IGenerator from "./generator.interface";

export default abstract class RepositoryGenerator<Entity> implements IGenerator<Entity> {

	public abstract async next(): Promise<Entity>;
	public abstract async count(): Promise<number>;

	public async toArray() {
		let e: Entity;
		const result: Entity[] = [];
		while (e = await this.next()) {
			result.push(e);
		}

		return result;
	}
}
