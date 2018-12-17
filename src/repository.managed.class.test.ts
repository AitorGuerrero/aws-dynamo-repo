import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import {EventEmitter} from "events";
import {beforeEach, describe, it} from "mocha";
import DynamoEntityManager from "./entity-manager/entity-manager.class";
import FakeDocumentClient from "./fake-document-client.class";
import {RepositoryManaged} from "./repository.managed.class";

import DocumentClient = DynamoDB.DocumentClient;

describe("Having a entity manager", () => {

	const keySchema = {hash: "id"};

	class Entity {
		public updated: boolean;
		public toDelete: boolean;
		public nested = {nestedUpdated: false, nestedToDelete: true};
		constructor(public id: string) {}
	}

	function unMarshal(m: any): Entity {
		const e = new Entity(m.id);
		e.updated = m.updated;
		e.toDelete = m.toDelete;
		e.nested = m.nested;

		return e;
	}

	function marshal(e: Entity) {
		return JSON.parse(JSON.stringify(e));
	}

	const tableName = "tableName";
	const entityId = "entityId";

	let documentClient: FakeDocumentClient;
	let entityManager: DynamoEntityManager;
	let repository: RepositoryManaged<Entity>;

	beforeEach(async () => {
		documentClient = new FakeDocumentClient({[tableName]: keySchema});
		entityManager = new DynamoEntityManager(
			documentClient as any as DocumentClient,
			new EventEmitter(),
		);
		repository = new RepositoryManaged(
			{
				keySchema: {hash: "id"},
				marshal,
				tableName,
				unMarshal,
			},
			documentClient as any as DocumentClient,
			entityManager,
		);
	});

	describe("and having a entity in document client", () => {

		let entity: Entity;
		let marshaledEntity: Entity;

		beforeEach(async () => {
			marshaledEntity = {
				id: entityId,
				nested: {nestedUpdated: false, nestedToDelete: true},
				toDelete: true,
				updated: false,
			};
			await documentClient.set(tableName, marshaledEntity);
			entity = await repository.get({id: entityId});
		});

		describe("and updating a nested attribute", () => {
			beforeEach(async () => entity.nested.nestedUpdated = true);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					expect(item.nested.nestedUpdated).to.be.true;
				});
			});
		});

		describe("and updating a attribute", () => {
			beforeEach(async () => entity.updated = true);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					expect(item.updated).to.be.true;
				});
			});
		});

		describe("and deleting a attribute", () => {
			beforeEach(async () => entity.toDelete = undefined);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					expect(item.toDelete).to.be.undefined;
				});
			});
		});

		describe("and deleting a nested attribute", () => {
			beforeEach(async () => entity.nested.nestedToDelete = undefined);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					console.log(JSON.stringify(item));
					expect(item.nested.nestedToDelete).to.be.undefined;
				});
			});
		});

		describe("and the entity is same as original", () => {
			it("should not update the item in the collection", async () => {
				documentClient.failOnCall();
				await entityManager.flush();
			});
		});

		describe("and deleting a entity", () => {
			it("should remove it from collection", async () => {
				entityManager.delete(tableName, entity);
				await entityManager.flush();
				expect(await documentClient.getByKey(tableName, {id: entityId})).to.be.undefined;
			});
		});
	});

	describe("when persisting a new entity", () => {
		const newId = "newId";
		let entity: Entity;
		beforeEach(() => {
			entity = new Entity(newId);
			entityManager.add(tableName, entity);
		});
		describe("and flushed", () => {
			beforeEach(() => entityManager.flush());
			it("should save the item in the collection", async () => {
				const item = await documentClient.getByKey<Entity>(tableName, {id: newId});
				expect(item).not.to.be.undefined;
			});
			it("should marshal the item", async () => {
				const item = await documentClient.getByKey<Entity>(tableName, {id: newId});
				expect(item).not.to.be.instanceOf(Entity);
			});
			describe("and deleting it", () => {
				beforeEach(() => entityManager.delete(tableName, entity));
				describe("and flushed", () => {
					beforeEach(() => entityManager.flush());
					it("Should not be added to the collection", async () => {
						expect(await documentClient.getByKey(tableName, {id: entityId})).to.be.undefined;
					});
				});
			});
		});
	});
});
