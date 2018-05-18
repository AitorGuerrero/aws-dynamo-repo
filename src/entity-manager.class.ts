/* tslint:disable:ban-types */
import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import getEntityKey from "./get-entity-key";

import DocumentClient = DynamoDB.DocumentClient;

type Action = "CREATE" | "UPDATE" | "DELETE";

export interface IEntityManagerTableConfig<Entity> {
	tableName: string;
	keySchema: DocumentClient.KeySchema;
	marshal: (entity: Entity) => DocumentClient.AttributeMap;
}

export interface IEntity<E> {
	constructor: Function;
}

export enum eventType {
	flushed = "flushed",
	errorCreating = "error.creating",
	errorUpdating = "error.updating",
	errorDeleting = "error.deleting",
	errorFlushing = "error.flushing",
}

type TrackedTable = Map<any, {action: Action, initialStatus?: any, entity: any, entityName: string}>;

export default class DynamoEntityManager {

	private readonly tableConfigs: Map<string, IEntityManagerTableConfig<any>>;
	private tracked: TrackedTable;

	constructor(
		private dc: DocumentClient,
		public readonly eventEmitter: EventEmitter,
	) {
		this.tracked = new Map();
		this.tableConfigs = new Map();
	}

	public addTableConfig(entityName: string, config: IEntityManagerTableConfig<any>) {
		this.tableConfigs.set(entityName, config);
	}

	public async flush() {
		const processed: Array<Promise<any>> = [];
		for (const entityConfig of this.tracked.values()) {
			switch (entityConfig.action) {
				case "UPDATE":
					processed.push(this.updateItem(entityConfig.entityName, entityConfig.entity));
					break;
				case "DELETE":
					processed.push(this.deleteItem(entityConfig.entityName, entityConfig.entity));
					break;
				case "CREATE":
					processed.push(this.createItem(entityConfig.entityName, entityConfig.entity));
					break;
			}
		}
		try {
			await Promise.all(processed);
		} catch (err) {
			this.eventEmitter.emit(eventType.errorFlushing);

			throw err;
		}
		this.eventEmitter.emit(eventType.flushed);
	}

	public track<E>(entityName: string, entity: E & IEntity<E>) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "UPDATE", initialStatus: JSON.stringify(entity), entity, entityName});
	}

	public add<E>(entityName: string, entity: E & IEntity<E>) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "CREATE", entity, entityName});
	}

	public delete<E>(entityName: string, entity: E & IEntity<E>) {
		if (entity === undefined) {
			return;
		}
		if (
			this.tracked.has(entity)
			&& this.tracked.get(entity).action === "CREATE"
		) {
			this.tracked.delete(entity);
		} else {
			this.tracked.set(entity, {action: "DELETE", entity, entityName});
		}
	}

	public clear() {
		this.tracked = new Map();
	}

	private async createItem<E>(entityName: string, entity: E & IEntity<E>) {
		const request = {
			Item: this.tableConfigs.get(entityName).marshal(entity),
			TableName: this.tableConfigs.get(entityName).tableName,
		};
		try {
			await this.asyncPut(request);
		} catch (err) {
			this.eventEmitter.emit(eventType.errorCreating, err, entity);

			throw err;
		}
	}

	private async updateItem<E>(entityName: string, entity: E & IEntity<E>) {
		if (!this.entityHasChanged(entity)) {
			return;
		}
		const request = {
			Item: this.tableConfigs.get(entityName).marshal(entity),
			TableName: this.tableConfigs.get(entityName).tableName,
		};
		try {
			await this.asyncPut(request);
		} catch (err) {
			this.eventEmitter.emit(eventType.errorUpdating, err, entity);

			throw err;
		}
	}

	private entityHasChanged<E>(entity: E & IEntity<E>) {
		return JSON.stringify(entity) !== this.tracked.get(entity).initialStatus;
	}

	private async deleteItem<E>(entityName: string, item: E & IEntity<E>) {
		try {
			return this.asyncDelete({
				Key: getEntityKey(this.tableConfigs.get(entityName).keySchema, item),
				TableName: this.tableConfigs.get(entityName).tableName,
			});
		} catch (err) {
			this.eventEmitter.emit(eventType.errorDeleting, err, item);

			throw err;
		}
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
