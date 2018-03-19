import {DynamoDB} from "aws-sdk";
import {Console} from "console";
import {setTimeout} from "timers";
import generatorToArray from "./generator-to-array";
import {IDynamoDBRepository, IGenerator, ISearchInput} from "./repository.class";
import TrackedEntitiesCollisionError from "./tracked-entities-collision.error";

import DocumentClient = DynamoDB.DocumentClient;

export interface IPersistingRepository<Entity> {
	persist: (e: Entity) => any;
	flush: () => Promise<void>;
}

export default class DynamoEntityManager<Entity>
	implements IDynamoDBRepository<Entity>, IPersistingRepository<Entity> {

	public waitBetweenRequests = 0;
	public waitBetweenTries = 1000;
	public maxTries = 3;

	private queueFreePromise: Promise<any>;
	private tracked: Map<string, Entity>;
	private initialStatus: Map<string, string>;
	private marshal: (e: Entity) => DocumentClient.AttributeMap;
	private deleted: Map<string, Entity>;
	private console: Console;

	constructor(
		private repo: IDynamoDBRepository<Entity>,
		private dc: DocumentClient,
		private tableName: string,
		marshal?: (entity: Entity) => DocumentClient.AttributeMap,
		customConsole?: Console,
	) {
		this.initialStatus = new Map();
		this.tracked = new Map();
		this.deleted = new Map();
		this.marshal = marshal != undefined ? marshal : defaultMarshal;
		this.queueFreePromise = Promise.resolve();
		this.console = customConsole || console;
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
		if (this.tracked.size > 0 || this.deleted.size > 0) {
			this.console.warn("Making a dynamo entity manager search with items not flushed. This will " +
			"result in error in future versions.");
		}
		const getNextEntity = this.repo.search(input);
		const mustTrack = input.ProjectionExpression === undefined;
		const generator = (async () => {
			const entity = await getNextEntity();
			if (mustTrack) {
				this.track(entity);
			}

			return entity;
		}) as IGenerator<Entity>;
		generator.toArray = generatorToArray;

		return generator;
	}

	public persist(entity: Entity) {
		this.track(entity, true);
		this.addToCache(entity);
	}

	public addToCache(entity: Entity) {
		this.repo.addToCache(entity);
	}

	public clear() {
		this.tracked.clear();
		this.deleted.clear();
		this.repo.clear();
	}

	public async flush() {
		const changedEntities = this.getChangedEntities();
		for (const entity of (changedEntities)) {
			await this.updateItem(entity);
		}
		for (const deleted of this.deleted.values()) {
			await this.deleteItem(deleted);
		}
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
				await this.asyncPut(request);
				saved = true;
			} catch (err) {
				if (tries++ > this.maxTries) {
					throw err;
				}
				await new Promise((rs) => setTimeout(rs, this.waitBetweenTries));
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

	private async deleteItem(item: Entity) {
		return this.asyncDelete({
			Key: this.repo.getEntityKey(item),
			TableName: this.tableName,
		});
	}

	private asyncPut(request: DocumentClient.PutItemInput) {
		return new Promise<DocumentClient.PutItemOutput>(
			(rs, rj) => this.dc.put(request, (err, res) => err ? rj(err) : rs(res)),
		);
	}

	private asyncDelete(request: DocumentClient.DeleteItemInput) {
		return new Promise<DocumentClient.DeleteItemOutput>(
			(rs, rj) => this.dc.delete(request, (err) => err ? rj(err) : rs()),
		);
	}
}

function defaultMarshal(e: any) {
	return JSON.parse(JSON.stringify(e));
}
