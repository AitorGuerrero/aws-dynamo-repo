import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import {DynamoEntityManager, ParallelFlusher} from "dynamo-entity-manager";
import {EventEmitter} from "events";
import {beforeEach, describe, it} from "mocha";
import PoweredDynamo from "powered-dynamo";
import FakeDocumentClient from "../fake-document-client.class";
import DynamoManagedRepository from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

describe("Having a managed repository", () => {

	const keySchema = {hash: "id"};

	class Entity {
		public updated = false;
		public toDelete = false;
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

	const tableName = "tableName";
	const entityId = "entityId";

	let documentClient: FakeDocumentClient;
	let entityManager: DynamoEntityManager;
	let repository: DynamoManagedRepository<Entity>;

	beforeEach(async () => {
		documentClient = new FakeDocumentClient({[tableName]: keySchema});
		entityManager = new DynamoEntityManager(
			new ParallelFlusher(new PoweredDynamo(documentClient as any as DocumentClient)),
			[{
				keySchema: {hash: "id", range: undefined},
				tableName,
			}],
			new EventEmitter(),
		);
		repository = new DynamoManagedRepository(
			{
				keySchema: {hash: "id"},
				tableName,
				unMarshal,
			},
			new PoweredDynamo(documentClient as any as DocumentClient),
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
			entity = (await repository.get({id: entityId}))!;
		});

		describe("and searching for a entity already cached", () => {
			let firstReturnedEntity: Entity;
			let entities: Entity[];
			beforeEach(async () => {
				firstReturnedEntity = (await repository.get({id: entityId}))!;
				entities = await (await repository.scan({})).toArray();
			});
			it("should return a entity", () => expect(entities.length).to.be.eq(1));
			it("should return the same entity", () => expect(entities[0]).to.be.equal(firstReturnedEntity));
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
			beforeEach(() => entity.toDelete = undefined as any);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					expect(item.toDelete).to.be.undefined;
				});
			});
		});

		describe("and deleting a nested attribute", () => {
			beforeEach(async () => entity.nested.nestedToDelete = undefined as any);
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
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
		beforeEach(async () => {
			entity = new Entity(newId);
			await repository.trackNew(entity);
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
			describe("and asking for the entity", () => {
				let loadedEntity: Entity;
				beforeEach(async () => loadedEntity = (await repository.get({id: entity.id}))!);
				it("should return the same entity", () => {
					expect(loadedEntity).to.be.equal(entity);
				});
			});
		});
	});

	describe("and asking for not existent entity", () => {
		const notExistingEntityId = "entityId";
		beforeEach(() => repository.get({id: notExistingEntityId}));
		describe("and tracking the entity as new", () => {
			let entity: Entity;
			beforeEach(async () => {
				entity = new Entity(notExistingEntityId);
				await repository.trackNew(entity);
			});
			describe("and asking for the new tracked entity", () => {
				let loadedEntity: Entity;
				beforeEach(async () => loadedEntity = (await repository.get({id: notExistingEntityId}))!);
				it("should return the tracked entity", () => {
					expect(loadedEntity).to.be.equal(entity);
				});
			});
		});
	});
});
