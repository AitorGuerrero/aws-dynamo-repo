import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import DynamoEntityManager from "./entity-manager.class";
import FakeDocumentClient from "./fake-document-client.class";
import {DynamoDBRepository} from "./repository.class";
import {RepositoryManaged} from "./repository.managed.class";

import DocumentClient = DynamoDB.DocumentClient;

describe("Having a entity manager", () => {

	const keySchema = [{AttributeName: "id", KeyType: "HASH"}];

	class Entity {
		public updated: boolean;
		constructor(public id: string) {}
	}

	function unMarshal(m: {id: string, updated: boolean}): Entity {
		return new Entity(m.id);
	}

	function marshal(e: Entity) {
		return {id: e.id, updated: e.updated};
	}

	const tableName = "tableName";
	const entityId = "entityId";

	let documentClient: FakeDocumentClient;
	let entityManager: DynamoEntityManager;
	let repository: RepositoryManaged<Entity>;

	beforeEach(async () => {
		documentClient = new FakeDocumentClient({[tableName]: keySchema});
		await documentClient.set(tableName, {id: "second", flag: 20, a: {b: 3, c: 4}});
		await documentClient.set(tableName, {id: "third", flag: 30, a: {b: 5, c: 6}});
		entityManager = new DynamoEntityManager(documentClient as any as DocumentClient);
		entityManager.addTableConfig({
			class: Entity,
			keySchema,
			marshal,
			tableName,
		});
		repository = new RepositoryManaged(
			tableName,
			new DynamoDBRepository<Entity>(
				documentClient as any as DocumentClient,
				{
					keySchema,
					tableName,
				},
				unMarshal,
			),
			entityManager,
		);
	});

	describe("and having a entity in document client", () => {

		let entity: Entity;
		let marshaledEntity: Entity;

		beforeEach(async () => {
			marshaledEntity = {id: entityId, updated: false};
			await documentClient.set(tableName, marshaledEntity);
			entity = await repository.get({id: entityId});
		});

		describe("when updating a entity", () => {
			beforeEach(() => {
				entity.updated = true;
			});
			describe("and flushed", () => {
				beforeEach(() => entityManager.flush());
				it("should update the item in the collection", async () => {
					const item = await documentClient.getByKey<Entity>(tableName, {id: entityId});
					expect(item.updated).to.be.true;
				});
			});
		});

		describe("when the entity is same as original", () => {
			it("should not update the item in the collection", async () => {
				documentClient.failOnCall();
				await entityManager.flush();
			});
		});

		describe("when deleting a entity", () => {
			it("should remove it from collection", async () => {
				entityManager.delete(entity);
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
			entityManager.add(entity);
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
				beforeEach(() => entityManager.delete(entity));
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
