/* tslint:disable:ban-types */
import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import getEntityKey from "./get-entity-key";
import {IRepositoryTableConfig} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

type Action = "CREATE" | "UPDATE" | "DELETE";

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

interface ITrackedITem {
	action: Action;
	initialStatus?: any;
	entity: any;
	entityName: string;
}

type TrackedTable = Map<any, ITrackedITem>;

export default class DynamoEntityManager {

	private readonly tableConfigs: Map<string, IRepositoryTableConfig<any>>;
	private tracked: TrackedTable;
	private flushing = false;

	constructor(
		private dc: DocumentClient,
		public readonly eventEmitter: EventEmitter,
	) {
		this.tracked = new Map();
		this.tableConfigs = new Map();
	}

	public addTableConfig(entityName: string, config: IRepositoryTableConfig<any>) {
		this.tableConfigs.set(entityName, Object.assign({
			marshal: (entity: any) => JSON.parse(JSON.stringify(entity)) as DocumentClient.AttributeMap,
		}, config));
	}

	public async flush() {
		this.guardFlushing();
		this.flushing = true;
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
			this.flushing = false;
			this.eventEmitter.emit(eventType.errorFlushing);

			throw err;
		}
		this.updateTrackedStatus();
		this.flushing = false;
		this.eventEmitter.emit(eventType.flushed);
	}

	public updateTrackedStatus() {
		this.tracked.forEach((value, key) => {
			switch (value.action) {
				case "CREATE":
					value.action = "UPDATE";
					value.initialStatus = JSON.stringify(value.entity);
					break;
				case "UPDATE":
					value.initialStatus = JSON.stringify(value.entity);
					break;
				case "DELETE":
					this.tracked.delete(key);
					break;
			}
		});
	}

	public track<E>(entityName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "UPDATE", initialStatus: JSON.stringify(entity), entity, entityName});
	}

	public add<E>(entityName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "CREATE", entity, entityName});
	}

	public delete<E>(entityName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
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
		this.guardFlushing();
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
		const tableConfig = this.tableConfigs.get(entityName);
		try {
			return this.asyncDelete({
				Key: getEntityKey(tableConfig.keySchema, tableConfig.marshal(item)),
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

	private guardFlushing() {
		if (this.flushing) {
			throw new Error("Dynamo entity manager currently flushing");
		}
	}
}
