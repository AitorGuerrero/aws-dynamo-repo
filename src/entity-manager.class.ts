/* tslint:disable:ban-types */
import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import getEntityKey from "./get-entity-key";
import {IRepositoryTableConfig} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

export interface IEntity<E> {
	constructor: Function;
}

enum Action {create, update, delete}

export enum eventType {
	flushed = "flushed",
	errorCreating = "error.creating",
	errorUpdating = "error.updating",
	errorDeleting = "error.deleting",
	errorFlushing = "error.flushing",
}

interface ITrackedITem<Entity> {
	action: Action;
	initialStatus?: unknown;
	entity: Entity;
	entityName: string;
}

type TrackedTable<E> = Map<any, ITrackedITem<E>>;

export default class DynamoEntityManager {

	private readonly tableConfigs: Map<string, IRepositoryTableConfig<any>>;
	private tracked: TrackedTable<unknown>;
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
			processed.push(this.flushEntity(entityConfig, this.tableConfigs.get(entityConfig.entityName)));
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
				case Action.create:
					value.action = Action.update;
					value.initialStatus = JSON.stringify(value.entity);
					break;
				case Action.update:
					value.initialStatus = JSON.stringify(value.entity);
					break;
				case Action.delete:
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
		this.tracked.set(entity, {action: Action.update, initialStatus: JSON.stringify(entity), entity, entityName});
	}

	public add<E>(entityName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: Action.create, entity, entityName});
	}

	public delete<E>(entityName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (
			this.tracked.has(entity)
			&& this.tracked.get(entity).action === Action.create
		) {
			this.tracked.delete(entity);
		} else {
			this.tracked.set(entity, {action: Action.delete, entity, entityName});
		}
	}

	public clear() {
		this.guardFlushing();
		this.tracked = new Map();
	}

	private flushEntity<E>(entityConfig: ITrackedITem<E>, tableConfig: IRepositoryTableConfig<E>) {
		switch (entityConfig.action) {
			case Action.update:
				return this.updateItem(entityConfig.entity, tableConfig);
			case Action.delete:
				return this.deleteItem(entityConfig.entity, tableConfig);
			case Action.create:
				return this.createItem(entityConfig.entity, tableConfig);
		}
	}

	private async createItem<E>(entity: E & IEntity<E>, tableConfig: IRepositoryTableConfig<E>) {
		const request: DocumentClient.PutItemInput = {
			ConditionExpression: "",
			Item: tableConfig.marshal(entity),
			TableName: tableConfig.tableName,
		};
		try {
			await this.asyncPut(request);
		} catch (err) {
			this.eventEmitter.emit(eventType.errorCreating, err, entity);

			throw err;
		}
	}

	private async updateItem<E>(entity: E & IEntity<E>, tableConfig: IRepositoryTableConfig<E>) {
		if (!this.entityHasChanged(entity)) {
			return;
		}
		const request = {
			Item: tableConfig.marshal(entity),
			TableName: tableConfig.tableName,
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

	private async deleteItem<E>(item: E & IEntity<E>, tableConfig: IRepositoryTableConfig<E>) {
		try {
			return this.asyncDelete({
				Key: getEntityKey(tableConfig.keySchema, tableConfig.marshal(item)),
				TableName: tableConfig.tableName,
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
