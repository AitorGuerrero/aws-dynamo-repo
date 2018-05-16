/* tslint:disable:ban-types */

import {DynamoDB} from "aws-sdk";
import {setTimeout} from "timers";
import getEntityKey from "./get-entity-key";

import DocumentClient = DynamoDB.DocumentClient;

type Action = "CREATE" | "UPDATE" | "DELETE";

export interface ITableConfig<Entity> {
	tableName: string;
	class: {new(...args: any[]): Entity};
	keySchema: DocumentClient.KeySchema;
	marshal: (entity: Entity) => DocumentClient.AttributeMap;
}

type TrackedTable = Map<any, {action: Action, initialStatus?: any, entity: any}>;

export default class DynamoEntityManager {

	public waitBetweenRequests = 0;
	public waitBetweenTries = 500;
	public maxTries = 3;

	private readonly tableConfigs: Map<{new(...args: any[]): any}, ITableConfig<any>>;
	private queueFreePromise: Promise<any>;
	private tracked: TrackedTable;

	constructor(
		private dc: DocumentClient,
	) {
		this.tracked = new Map();
		this.tableConfigs = new Map();
		this.queueFreePromise = Promise.resolve();
	}

	public addTableConfig(config: ITableConfig<any>) {
		this.tableConfigs.set(config.class, config);
	}

	public async flush() {
		for (const entityConfig of this.tracked.values()) {
			switch (entityConfig.action) {
				case "UPDATE":
					await this.updateItem(entityConfig.entity);
					break;
				case "DELETE":
					await this.deleteItem(entityConfig.entity);
					break;
				case "CREATE":
					await this.createItem(entityConfig.entity);
					break;
			}
		}
	}

	public track<Entity>(entity: Entity & {constructor: {new(...args: any[]): Entity}}) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "UPDATE", initialStatus: JSON.stringify(entity), entity});
	}

	public add<Entity>(entity: Entity & {constructor: Function}) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.has(entity)) {
			return;
		}
		this.tracked.set(entity, {action: "CREATE", entity});
	}

	public delete<Entity>(entity: Entity & {constructor: Function}) {
		if (entity === undefined) {
			return;
		}
		if (
			this.tracked.has(entity)
			&& this.tracked.get(entity).action === "CREATE"
		) {
			this.tracked.delete(entity);
		} else {
			this.tracked.set(entity, {action: "DELETE", entity});
		}
	}

	private async createItem<Entity>(entity: Entity & {constructor: {new(...args: any[]): Entity}}) {
		let tries = 1;
		let saved = false;
		const request = {
			Item: this.tableConfigs.get(entity.constructor).marshal(entity),
			TableName: this.tableConfigs.get(entity.constructor).tableName,
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

	private async updateItem<Entity>(entity: Entity & {constructor: {new(...args: any[]): Entity}}) {
		if (!this.entityHasChanged(entity)) {
			return;
		}
		let tries = 1;
		let saved = false;
		const request = {
			Item: this.tableConfigs.get(entity.constructor).marshal(entity),
			TableName: this.tableConfigs.get(entity.constructor).tableName,
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

	private entityHasChanged<Entity>(entity: Entity & {constructor: {new(...args: any[]): Entity}}) {
		return JSON.stringify(entity) !== this.tracked.get(entity).initialStatus;
	}

	private async deleteItem<Entity>(item: Entity & {constructor: {new(...args: any[]): Entity}}) {
		return this.asyncDelete({
			Key: getEntityKey(this.tableConfigs.get(item.constructor).keySchema, item),
			TableName: this.tableConfigs.get(item.constructor).tableName,
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
