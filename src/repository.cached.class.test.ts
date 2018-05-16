import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import FakeDocumentClient from "./fake-document-client.class";
import {RepositoryCached} from "./repository.cached.class";
import {DynamoDBRepository} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

describe("Having a repository with cache", () => {

	const keySchema = [{AttributeName: "id", KeyType: "HASH"}];

	interface IMarshaled {
		id: string;
		marshaledAttr: true;
	}

	interface IEntity {
		id: string;
		unMarshaledAttr: true;
	}

	const tableName = "tableName";

	let documentClient: FakeDocumentClient;
	let repository: RepositoryCached<IEntity>;

	function unMarshal(m: IMarshaled): IEntity {
		return {id: m.id, unMarshaledAttr: m.marshaledAttr};
	}

	beforeEach(() => {
		documentClient = new FakeDocumentClient({[tableName]: keySchema});
		repository = new RepositoryCached(
			new DynamoDBRepository<IEntity>(
				documentClient as any as DocumentClient,
				{
					keySchema,
					tableName,
				},
				unMarshal,
			),
			keySchema,
		);
	});

	describe("when asking for not existent entity", () => {

		const notExistentEntityId = "notExistentEntity";
		let returnedEntity: IEntity;

		beforeEach(async () => {
			returnedEntity = await repository.get({id: notExistentEntityId});
		});
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

		beforeEach(() => {
			repository.addToCache({id: entityId, unMarshaledAttr: true});
		});
		describe("and asking again for the entity", () => {
			let newReturnedEntity: IEntity;
			beforeEach(async () => newReturnedEntity = await repository.get({id: entityId}));
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

		let marshaledEntity: IMarshaled;

		beforeEach(async () => {
			marshaledEntity = {id: entityId, marshaledAttr: true};
			await documentClient.set(tableName, marshaledEntity);
			await documentClient.set(tableName, {id: entityId, marshaledAttr: true});
			await documentClient.set(tableName, {id: secondEntityId, marshaledAttr: true});
			await documentClient.set(tableName, {id: thirdEntityId, marshaledAttr: true});
		});

		describe("when asking for the entity", () => {

			let returnedEntity: IEntity;

			beforeEach(async () => returnedEntity = await repository.get(entityKey));

			it("should return the unmarshaled entity", async () => {
				expect(returnedEntity.id).to.be.eq(entityId);
				expect(returnedEntity.unMarshaledAttr).to.be.eq(true);
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
				const getNextEntity = repository.search({});
				const entity = await getNextEntity();
				expect(entity.id).eq(entityId);
				expect(entity.unMarshaledAttr).eq(true);
				expect((await getNextEntity()).id).to.be.eq(secondEntityId);
				expect((await getNextEntity()).id).to.be.eq(thirdEntityId);
				expect(await getNextEntity()).to.be.undefined;
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