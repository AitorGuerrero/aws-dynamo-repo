import IEntityResponse from "./entity-response.interface";
import IGenerator from "./generator.interface";

export default abstract class RepositoryGenerator<Entity> implements IGenerator<Entity> {

	public abstract async next(): Promise<IEntityResponse<Entity>>;
	public abstract async count(): Promise<number>;

	public async toArray() {
		let e: IEntityResponse<Entity>;
		const result: Array<IEntityResponse<Entity>> = [];
		while (e = await this.next()) {
			result.push(e);
		}

		return result;
	}
}
