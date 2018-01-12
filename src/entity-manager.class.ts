import {DynamoDB} from "aws-sdk";
import {setTimeout} from "timers";
import {IDynamoDBRepository, ISearchInput} from "./repository.class";
import TrackedEntitiesCollisionError from "./tracked-entities-collision.error";
import DocumentClient = DynamoDB.DocumentClient;

export interface IPersistingRepository<Entity> {
	persist: (e: Entity) => any;
	clear: () => any;
	flush: () => Promise<void>;
}

export default class DynamoEntityManager<Entity>
	implements IDynamoDBRepository<Entity>, IPersistingRepository<Entity> {

	public waitTimeBetweenTries = 1000;
	public  maxTries = 3;
	private tracked: Map<string, Entity>;
	private initialStatus: Map<string, string>;
	private marshal: (e: Entity) => DocumentClient.AttributeMap;
	private deleted: Map<string, Entity>;

	constructor(
		private repo: IDynamoDBRepository<Entity>,
		private dc: DocumentClient,
		private tableName: string,
		marshal?: (entity: Entity) => DocumentClient.AttributeMap,
	) {
		this.initialStatus = new Map();
		this.tracked = new Map();
		this.deleted = new Map();
		this.marshal = marshal !== undefined ? marshal : defaultMarshal;
	}

	public async get(key: DocumentClient.Key) {
		const response = await this.repo.get(key);
		this.track(response);

		return response;
	}

	public async getList(keys: DocumentClient.Key[]) {
		const response = await this.repo.getList(keys);
		for (const entity of response.values()) {
			this.track(entity);
		}

		return response;
	}

	public search(input: ISearchInput) {
		const getNextEntity = this.repo.search(input);

		return async () => {
			const entity = await getNextEntity();
			this.track(entity);

			return entity;
		};
	}

	public persist(entity: Entity) {
		this.track(entity, true);
	}

	public clear() {
		this.tracked.clear();
	}

	public async flush() {
		const changedEntities = this.getChangedEntities();
		const processed: Array<Promise<any>> = [];
		for (const entity of (changedEntities)) {
			processed.push(this.updateItem(entity));
		}
		for (const deleted of this.deleted.values()) {
			processed.push(this.deleteItem(deleted));
		}

		await Promise.all(processed);
	}

	public getEntityId(entity: Entity) {
		return this.repo.getEntityId(entity);
	}

	public getEntityKey(entity: Entity) {
		return this.repo.getEntityKey(entity);
	}

	public delete(entity: Entity) {
		this.deleted.set(this.getEntityId(entity), entity);
	}

	private async updateItem(entity: Entity) {
		if (this.deleted.get(this.getEntityId(entity))) {
			return;
		}
		let tries = 1;
		let saved = false;
		const request = {TableName: this.tableName, Item: this.marshal(entity)};
		while (saved === false) {
			try {
				await new Promise<DocumentClient.PutItemOutput>(
					(rs, rj) => this.dc.put(request, (err, res) => err ? rj(err) : rs(res)),
				);
				saved = true;
			} catch (err) {
				if (tries++ < this.maxTries) {
					await new Promise((rs) => setTimeout(() => rs(), this.waitTimeBetweenTries));
				} else {
					throw err;
				}
			}
		}
	}

	private getChangedEntities() {
		const changedEntities: Entity[] = [];
		for (const entity of this.tracked.values()) {
			const id = this.repo.getEntityId(entity);
			if (
				false === this.initialStatus.has(id)
				|| JSON.stringify(entity) !== this.initialStatus.get(id)
			) {
				changedEntities.push(entity);
			}
		}

		return changedEntities;
	}

	private track(entity: Entity, isNew?: boolean) {
		if (entity === undefined) {
			return;
		}
		const id = this.repo.getEntityId(entity);
		if (this.tracked.has(id)) {
			if (this.tracked.get(id) !== entity) {
				throw new TrackedEntitiesCollisionError(this.tracked.get(id), entity);
			}

			return;
		}
		this.tracked.set(id, entity);
		if (isNew !== true) {
			this.initialStatus.set(id, JSON.stringify(entity));
		}
	}

	private deleteItem(item: Entity) {
		return new Promise(
			(rs, rj) => this.dc.delete({
				Key: this.repo.getEntityKey(item),
				TableName: this.tableName,
			},
			(err) => err ? rj(err) : rs()),
		);
	}
}

function defaultMarshal(e: any) {
	return JSON.parse(JSON.stringify(e));
}
