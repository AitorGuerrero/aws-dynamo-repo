/* tslint:disable:ban-types */
import {DynamoDB} from "aws-sdk";
import {EventEmitter} from "events";
import {setTimeout} from "timers";
import getEntityKey from "./get-entity-key";

import DocumentClient = DynamoDB.DocumentClient;

type Action = "CREATE" | "UPDATE" | "DELETE";

export interface ITableConfig<Entity> {
	tableName: string;
	keySchema: DocumentClient.KeySchema;
	marshal: (entity: Entity) => DocumentClient.AttributeMap;
}

export interface IEntity<E> {
	constructor: Function;
}

export enum event {
	flushed = "flushed",
}

type TrackedTable = Map<any, {action: Action, initialStatus?: any, entity: any, entityName: string}>;

export default class DynamoEntityManager {

	public waitBetweenTries = 500;
	public maxTries = 3;

	private readonly tableConfigs: Map<string, ITableConfig<any>>;
	private tracked: TrackedTable;

	constructor(
		private dc: DocumentClient,
		public readonly eventEmitter: EventEmitter,
	) {
		this.tracked = new Map();
		this.tableConfigs = new Map();
	}

	public addTableConfig(entityName: string, config: ITableConfig<any>) {
		this.tableConfigs.set(entityName, config);
	}

	public async flush() {
		for (const entityConfig of this.tracked.values()) {
			switch (entityConfig.action) {
				case "UPDATE":
					await this.updateItem(entityConfig.entityName, entityConfig.entity);
					break;
				case "DELETE":
					await this.deleteItem(entityConfig.entityName, entityConfig.entity);
					break;
				case "CREATE":
					await this.createItem(entityConfig.entityName, entityConfig.entity);
					break;
			}
		}
		this.eventEmitter.emit(event.flushed);
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
		let tries = 1;
		let saved = false;
		const request = {
			Item: this.tableConfigs.get(entityName).marshal(entity),
			TableName: this.tableConfigs.get(entityName).tableName,
		};
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

	private async updateItem<E>(entityName: string, entity: E & IEntity<E>) {
		if (!this.entityHasChanged(entity)) {
			return;
		}
		let tries = 1;
		let saved = false;
		const request = {
			Item: this.tableConfigs.get(entityName).marshal(entity),
			TableName: this.tableConfigs.get(entityName).tableName,
		};
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

	private entityHasChanged<E>(entity: E & IEntity<E>) {
		return JSON.stringify(entity) !== this.tracked.get(entity).initialStatus;
	}

	private async deleteItem<E>(entityName: string, item: E & IEntity<E>) {
		return this.asyncDelete({
			Key: getEntityKey(this.tableConfigs.get(entityName).keySchema, item),
			TableName: this.tableConfigs.get(entityName).tableName,
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
