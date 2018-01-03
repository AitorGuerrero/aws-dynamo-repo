import {DocumentClient} from "aws-sdk/lib/dynamodb/document_client";
import {setTimeout} from "timers";
import {IDynamoDBRepository, ISearchInput} from "./repository.class";
import TrackedEntitiesCollisionError from "./tracked-entities-collision.error";

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

	constructor(
		private repo: IDynamoDBRepository<Entity>,
		private dc: DocumentClient,
		private marshal: (entity: Entity) => DocumentClient.AttributeMap,
		private tableName: string,
	) {
		this.initialStatus = new Map();
		this.tracked = new Map();
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

		await Promise.all(processed);
	}

	public getEntityId(entity: Entity) {
		return this.repo.getEntityId(entity);
	}

	private async updateItem(entity: Entity) {
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
}
