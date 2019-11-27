import IEntityGenerator from "../generator.interface";
import RepositoryManaged from "./repository.class";

export default class ManagedRepositoryGenerator<Entity> implements IEntityGenerator<Entity> {

	constructor(
		protected repository: RepositoryManaged<Entity>,
		private generator: IEntityGenerator<Entity>,
	) {}

	public async next() {
		const response = await this.generator.next();
		if (response === undefined) {
			return;
		}
		await this.repository.track(response);

		return response;
	}

	public count() {
		return this.generator.count();
	}

	public async toArray(): Promise<Entity[]> {
		const entities: Entity[] = [];
		let entity: Entity;
		while (entity = await this.next()) {
			entities.push(entity);
		}

		return entities;
	}

	public async slice(amount: number): Promise<Entity[]> {
		const items = await this.generator.slice(amount);
		for (const item of items) {
			await this.repository.track(item);
		}

		return items;
	}
}
