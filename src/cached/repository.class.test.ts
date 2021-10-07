import {DocumentClient} from "aws-sdk/clients/dynamodb";
import {expect} from "chai";
import {beforeEach, describe, it} from "mocha";
import PoweredDynamo from "powered-dynamo";
import FakeDocumentClient from "../fake-document-client.class";
import DynamoCachedRepository from "./repository.class";
import {DynamoDB} from 'aws-sdk';

describe("Having a repository with cache", () => {

	const keySchema = {hash: "id"};

	interface Marshaled {
		id: string;
		marshaled: true;
	}

	interface Entity {
		id: string;
		marshaled: false;
	}

	const tableName = "tableName";

	let documentClient: FakeDocumentClient;
	let repository: DynamoCachedRepository<Entity>;

	function unMarshal(m: DynamoDB.DocumentClient.AttributeMap): Entity {
		return Object.assign(JSON.parse(JSON.stringify(m)), {marshaled: false});
	}

	function marshal(e: Entity): Marshaled {
		return Object.assign(JSON.parse(JSON.stringify(e)), {marshaled: true});
	}

	beforeEach(() => {
		documentClient = new FakeDocumentClient({[tableName]: keySchema});
		repository = new DynamoCachedRepository(
			new PoweredDynamo(documentClient as any as DocumentClient),
			{
				keySchema,
				marshal,
				tableName,
				unMarshal,
			},
		);
	});

	describe("when asking for not existent entity", () => {

		const notExistentEntityId = "notExistentEntity";
		let returnedEntity: undefined;

		beforeEach(async () => returnedEntity = (await repository.get({id: notExistentEntityId})) as undefined);
		it("should not return it", async () => {
			expect(returnedEntity).to.be.undefined;
		});

		describe("and asked for the same entity a second time", () => {
			it("Should not ask to the document client", async () => {
				documentClient.failOnCall();
				const entity = await repository.get({id: notExistentEntityId});
				expect(entity).to.be.undefined;
			});
		});
	});

	describe("when adding a entity to cache", () => {

		const entityId = "entityId";

		beforeEach(() => repository.addToCache({id: entityId, marshaled: false}));
		describe("and asking again for the entity", () => {
			let newReturnedEntity: Entity;
			beforeEach(async () => newReturnedEntity = (await repository.get({id: entityId})) as Entity);
			it(
				"should return correct entity",
				async () => {
					documentClient.flush();
					expect(newReturnedEntity.id).to.be.eq(entityId);
				},
			);
		});
	});

	describe("and some entities in the database", () => {

		const entityId = "entityId";
		const secondEntityId = "secondEntityId";
		const thirdEntityId = "thirdEntityId";
		const entityKey = {id: entityId};

		let marshaledEntity: Marshaled;

		beforeEach(async () => {
			marshaledEntity = {id: entityId, marshaled: true};
			await documentClient.set(tableName, marshaledEntity);
			await documentClient.set(tableName, {id: secondEntityId, marshaled: true});
			await documentClient.set(tableName, {id: thirdEntityId, marshaled: true});
		});

		describe("when asking for the entity", () => {

			let returnedEntity: Entity;

			beforeEach(async () => returnedEntity = (await repository.get(entityKey)) as Entity);

			it("should return the unmarshaled entity", async () => {
				expect(returnedEntity.id).to.be.eq(entityId);
				expect(returnedEntity.marshaled).to.be.eq(false);
			});
		});

		describe("when asking for some entities and some of them doesn't exists", () => {

			const notExistentKey = {id: "notExistentId"};

			it("Should return only the existent ones", async () => {
				const entities = await repository.getList([entityKey, notExistentKey]);
				expect(entities.get(notExistentKey)).to.be.undefined;
				expect(entities.get(entityKey)).not.to.be.undefined;
			});
		});

		describe("when searching for entities", () => {
			it("should return the entities", async () => {
				const getNextEntity = await repository.scan({});
				const entities = await getNextEntity.toArray();
				expect(entities.length).eq(3);
				expect(entities[0].id).eq(entityId);
				expect(entities[0].marshaled).eq(false);
				expect(entities[1].id).to.be.eq(secondEntityId);
				expect(entities[2].id).to.be.eq(thirdEntityId);
			});
		});

		describe("when asking twice for a entity", () => {
			it("should return the same entity", async () => {
				const firstEntity = await repository.get({id: entityId});
				const secondEntity = await repository.get({id: entityId});
				expect(firstEntity).to.be.eq(secondEntity);
			});
		});

		describe("when asking a second time for a entity before the first call resolves", () => {
			it("should return the same entity", async () => {
				const firstEntity = await repository.get({id: entityId});
				const secondEntity = await repository.get({id: entityId});
				expect(firstEntity).to.be.eq(secondEntity);
			});
		});

		describe("when asked for a entity in a list that have been previously asked for", () => {
			it("Should return the same entity", async () => {
				const entity = await repository.get({id: entityId});
				const list = await repository.getList([{id: "second"}, entityKey, {id: "notExistent"}]);
				expect(entity).to.be.eq(list.get(entityKey));
			});
		});
	});
});
