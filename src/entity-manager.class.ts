import {DynamoDB} from "aws-sdk";
import {setTimeout} from "timers";
import getEntityKey from "./get-entity-key";

import DocumentClient = DynamoDB.DocumentClient;

type TableName = string;
type Action = "CREATE" | "UPDATE" | "DELETE";

export interface ITableConfigs {
	[tableName: string]: {
		keySchema: DocumentClient.KeySchema,
		marshal: (entity: any) => DocumentClient.AttributeMap;
	};
}

type Tracked = Map<TableName, TrackedTable<any>>;
type TrackedTable<Entity> = Map<Entity, {action: Action, initialStatus?: any}>;

export default class DynamoEntityManager {

	public waitBetweenRequests = 0;
	public waitBetweenTries = 500;
	public maxTries = 3;

	private queueFreePromise: Promise<any>;
	private tracked: Tracked;

	constructor(
		private dc: DocumentClient,
		private tableConfigs: ITableConfigs,
	) {
		this.tracked = new Map();
		for (const tableName in tableConfigs) {
			this.tracked.set(tableName, new Map());
		}
		this.queueFreePromise = Promise.resolve();
	}

	public async flush() {
		for (const tableName of this.tracked.keys()) {
			for (const entity of this.tracked.get(tableName).keys()) {
				switch (this.tracked.get(tableName).get(entity).action) {
					case "UPDATE":
						await this.updateItem(tableName, entity);
						break;
					case "DELETE":
						await this.deleteItem(tableName, entity);
						break;
					case "CREATE":
						await this.createItem(tableName, entity);
						break;
				}
			}
		}
	}

	public track(tableName: string, entity: any) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.get(tableName).has(entity)) {
			return;
		}
		this.tracked.get(tableName).set(entity, {action: "UPDATE", initialStatus: JSON.stringify(entity)});
	}

	public add(tableName: string, entity: any) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.get(tableName).has(entity)) {
			return;
		}
		this.tracked.get(tableName).set(entity, {action: "CREATE"});
	}

	public delete(tableName: string, entity: any) {
		if (entity === undefined) {
			return;
		}
		if (this.tracked.get(tableName).has(entity) && this.tracked.get(tableName).get(entity).action === "CREATE") {
			this.tracked.get(tableName).delete(entity);
		} else {
			this.tracked.get(tableName).set(entity, {action: "DELETE"});
		}
	}

	private async createItem(tableName: string, entity: any) {
		let tries = 1;
		let saved = false;
		const request = {
			Item: this.tableConfigs[tableName].marshal(entity),
			TableName: tableName,
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

	private async updateItem(tableName: string, entity: any) {
		if (!this.entityHasChanged(tableName, entity)) {
			return;
		}
		let tries = 1;
		let saved = false;
		const request = {TableName: tableName, Item: this.tableConfigs[tableName].marshal(entity)};
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

	private entityHasChanged(tableName: string, entity: any) {
		return JSON.stringify(entity) !== this.tracked.get(tableName).get(entity).initialStatus;
	}

	private async deleteItem(tableName: string, item: any) {
		return this.asyncDelete({
			Key: getEntityKey(this.tableConfigs[tableName].keySchema, item),
			TableName: tableName,
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
