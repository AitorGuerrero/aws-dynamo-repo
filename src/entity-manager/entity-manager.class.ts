/* tslint:disable:ban-types */
import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import ErrorFlushingEntity from "./error.flushing.class";

import DocumentClient = DynamoDB.DocumentClient;

export interface IEntity<E> {
	constructor: Function;
}

enum Action {create, update, delete}

export enum eventType {
	flushed = "flushed",
	error = "error",
}

interface ITrackedITem<Entity> {
	action: Action;
	initialStatus?: unknown;
	entity: Entity;
	tableConfig: ITableConfig<Entity>;
}

type TrackedTable<E> = Map<any, ITrackedITem<E>>;

interface ITableConfig<E> {
	tableName: string;
	keySchema: {
		hash: string;
		range?: string;
	};
	marshal?: (e: E) => DocumentClient.AttributeMap;
}

export default class DynamoEntityManager {

	private static buildKeyConditionExpression(entity: any, tableConf: ITableConfig<unknown>) {
		const result: any = {
			ConditionExpression: "#keyHash<>::keyHash",
			ExpressionAttributeNames: {"#keyHash": tableConf.keySchema.hash},
			ExpressionAttributeValues: {":keyHash": entity[tableConf.keySchema.hash]},
		};
		if (tableConf.keySchema.range !== undefined) {
			result.ConditionExpression = result.ConditionExpression + " and #keyRange<>:keyRange";
			result.ExpressionAttributeNames["#keyRange"] = tableConf.keySchema.range;
			result.ExpressionAttributeValues[":keyRange"] = entity[tableConf.keySchema.range];
		}

		return result;
	}

	private static getEntityKey<Entity>(entity: Entity, tableConfig: ITableConfig<unknown>) {
		const key: DocumentClient.Key = {};
		key[tableConfig.keySchema.hash] = (entity as any)[tableConfig.keySchema.hash];
		if (tableConfig.keySchema.range) {
			key[tableConfig.keySchema.range] = (entity as any)[tableConfig.keySchema.range];
		}

		return key;
	}

	private static createItem<E>(entity: E & IEntity<E>, tableConfig: ITableConfig<E>): DynamoDB.TransactWriteItem {
		const marshaledEntity = tableConfig.marshal(entity);
		return {
			Put: Object.assign(
				DynamoEntityManager.buildKeyConditionExpression(marshaledEntity, tableConfig),
				{
					Item: marshaledEntity,
					TableName: tableConfig.tableName,
				},
			),
		};
	}

	private static deleteItem<E>(item: E & IEntity<E>, tableConfig: ITableConfig<E>): DynamoDB.TransactWriteItem {
		return {
			Delete: {
				Key: DynamoEntityManager.getEntityKey(item, tableConfig),
				TableName: tableConfig.tableName,
			},
		};
	}

	private readonly tableConfigs: {[tableName: string]: ITableConfig<unknown>} = {};
	private tracked: TrackedTable<unknown> = new Map();
	private flushing = false;

	constructor(
		private dc: DocumentClient,
		public readonly eventEmitter: EventEmitter,
	) {
		this.tracked = new Map();
	}

	public addTableConfig(config: ITableConfig<unknown>) {
		this.tableConfigs[config.tableName] = Object.assign({
			marshal: (entity: any) => JSON.parse(JSON.stringify(entity)) as DocumentClient.AttributeMap,
		}, config);
	}

	public async flush() {
		this.guardFlushing();
		this.flushing = true;
		const operations: DocumentClient.TransactWriteItem[] = [];
		for (const entityConfig of this.tracked.values()) {
			operations.push(this.flushEntity(entityConfig, entityConfig.tableConfig));
		}
		try {
			await this.asyncTransaction({
				TransactItems: operations,
			});
		} catch (err) {
			this.flushing = false;
			this.eventEmitter.emit(eventType.error, new ErrorFlushingEntity(err));

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

	public track<E>(tableName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {
			action: Action.update,
			entity,
			initialStatus: JSON.stringify(entity),
			tableConfig: this.tableConfigs[tableName],
		});
	}

	public add<E>(tableName: string, entity: E & IEntity<E>) {
		this.guardFlushing();
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: Action.create, entity, tableConfig: this.tableConfigs[tableName]});
	}

	public delete<E>(tableName: string, entity: E & IEntity<E>) {
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
			this.tracked.set(entity, {action: Action.delete, entity, tableConfig: this.tableConfigs[tableName]});
		}
	}

	public clear() {
		this.guardFlushing();
		this.tracked = new Map();
	}

	private flushEntity<E>(entityConfig: ITrackedITem<E>, tableConfig: ITableConfig<E>): DynamoDB.TransactWriteItem {
		switch (entityConfig.action) {
			case Action.update:
				return this.updateItem(entityConfig.entity, tableConfig);
			case Action.delete:
				return DynamoEntityManager.deleteItem(entityConfig.entity, tableConfig);
			case Action.create:
				return DynamoEntityManager.createItem(entityConfig.entity, tableConfig);
		}
	}

	private updateItem<E>(entity: E & IEntity<E>, tableConfig: ITableConfig<E>): DocumentClient.TransactWriteItem {
		if (!this.entityHasChanged(entity)) {
			return;
		}

		return {
			Put: {
				Item: tableConfig.marshal(entity),
				TableName: tableConfig.tableName,
			},
		};
	}

	private entityHasChanged<E>(entity: E & IEntity<E>) {
		return JSON.stringify(entity) !== this.tracked.get(entity).initialStatus;
	}

	private asyncTransaction(request: DocumentClient.TransactWriteItemsInput) {
		return new Promise<DocumentClient.TransactWriteItemsOutput>(
			(rs, rj) => this.dc.transactWrite(request, (err, res) => err ? rj(err) : rs(res)),
		);
	}

	private guardFlushing() {
		if (this.flushing) {
			throw new Error("Dynamo entity manager currently flushing");
		}
	}
}
