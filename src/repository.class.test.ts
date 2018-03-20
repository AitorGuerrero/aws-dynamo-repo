import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import FakeDocumentClient from "./fake-document-client.class";
import {DynamoDBRepository, IDynamoDBRepository} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;

describe("Having a repository with cache", () => {

	const keySchema = [{AttributeName: "id", KeyType: "HASH"}];

	interface IMarshaled {
		id: string;
		flag: number;
	}

	interface IEntity {
		id: string;
		flag: number;
	}

	const tableName = "tableName";
	const entityId = "entityId";
	const flag = 10;

	let documentClient: FakeDocumentClient<IEntity>;
	let repository: IDynamoDBRepository<any>;
	let marshaledEntity: IMarshaled;

	function unMarshal(m: IMarshaled): IEntity {
		return {id: m.id, flag: unMarshalFlag(m.flag)};
	}

	function unMarshalFlag(f: number) {
		return f + 1;
	}

	beforeEach(() => {
		marshaledEntity = {id: entityId, flag};
		documentClient = new FakeDocumentClient(new Map(), tableName, keySchema);
		documentClient.set(marshaledEntity);
		documentClient.set({id: "second", flag: "secondFlag"});
		documentClient.set({id: "third", flag: "thirdFlag"});
		repository = new DynamoDBRepository<IEntity>(
			documentClient as any as DocumentClient,
			tableName,
			[{KeyType: "HASH", AttributeName: "id"}],
			unMarshal,
		);
	});

	describe("when asking for an entity", () => {

		const notExistentEntityId = "notExistentEntity";

		let returnedEntity: IEntity;

		beforeEach(async () => returnedEntity = await repository.get({id: notExistentEntityId}));

		describe("and the entity doesn't exists", () => {
			it("should return undefined", () => expect(returnedEntity).to.be.undefined);
			describe("and then adding a entity with that id to cache", () => {
				beforeEach(() => repository.addToCache({id: notExistentEntityId, flag: 1}));
				describe("and asking again for the entity", () => {
					let newReturnedEntity: IEntity;
					beforeEach(async () => newReturnedEntity = await repository.get({id: notExistentEntityId}));
					it.only(
						"should not return undefined",
						async () => expect(newReturnedEntity).not.to.be.undefined,
					);
					it.only(
						"should return correct entity",
						async () => expect(newReturnedEntity.id).to.be.eq(notExistentEntityId),
					);
				});
			});
		});

		it("should return the unmarshaled entity", async () => {
			const entity = await repository.get({id: entityId});
			expect(entity.id).to.be.eq(entityId);
			expect(entity.flag).to.be.eq(unMarshalFlag(flag));
		});
	});

	describe("when asking for some entities", () => {
		describe("and some of them doesn't exists", () => {
			it("Should return only the existent ones", async () => {
				const entityKey = {id: entityId};
				const entities = await repository.getList([entityKey, {id: "notExistentId"}]);
				expect(entities.get({id: "notExistentId"})).to.be.undefined;
				expect(entities.get(entityKey).flag).to.be.eq(unMarshalFlag(flag));
			});
		});
	});

	describe("when searching for entities", () => {
		it("should return the entities", async () => {
			const getNextEntity = repository.search({});
			const entity = await getNextEntity();
			expect(entity.id).eq(entityId);
			expect(entity.flag).eq(unMarshalFlag(flag));
			expect((await getNextEntity()).id).to.be.eq("second");
			expect((await getNextEntity()).id).to.be.eq("third");
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
			documentClient.stop();
			const firstEntityPromise = repository.get({id: entityId});
			const secondEntityPromise = repository.get({id: entityId});
			documentClient.resume();
			expect(await firstEntityPromise).to.be.eq(await secondEntityPromise);
		});
	});

	describe("when asked for a entity in a list that have been previously asked for", () => {
		it("Should return the same entity", async () => {
			const entityKey = {id: entityId};
			const entity = await repository.get({id: entityId});
			const list = await repository.getList([{id: "second"}, entityKey, {id: "notExistent"}]);
			expect(entity).to.be.eq(list.get(entityKey));
		});
	});

	describe("when asked for a not existent entity a second time", () => {
		it("Should not ask to the document client", async () => {
			await repository.get({id: "notExistent"});
			documentClient.failOnCall();
			const result = await repository.get({id: "notExistent"});
			expect(result).to.be.undefined;
		});
	});
});
