import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import FakeDocumentClient from "./fake-document-client.class";
import {DynamoDBRepository} from "./repository.class";

import DocumentClient = DynamoDB.DocumentClient;
import DynamoEntityManager from "./entity-manager.class";

describe("Having a entity manager", () => {

	const keySchema = [{AttributeName: "id", KeyType: "HASH"}];

	interface IMarshaled {
		id: string;
		flag: number;
		a: {b: number, c: number};
	}

	interface IEntity {
		id: string;
		flag: number;
		a: {b: number, c: number};
	}
	function unMarshal(m: IMarshaled): IEntity {
		return {id: m.id, flag: unMarshalFlag(m.flag), a: m.a};
	}

	function marshal(e: IEntity): IMarshaled {
		return {id: e.id, flag: marshalFlag(e.flag), a: e.a};
	}

	function marshalFlag(flag: number) {
		return flag + 1;
	}

	function unMarshalFlag(flag: number) {
		return flag - 1;
	}

	const tableName = "tableName";
	const entityId = "entityId";
	const originalFlag = 1;

	let documentClient: FakeDocumentClient<IEntity>;
	let entityManager: DynamoEntityManager<any>;
	let marshaledEntity: IMarshaled;
	let entity: IEntity;

	beforeEach(async () => {
		marshaledEntity = {id: entityId, flag: originalFlag, a: {b: 1, c: 2}};
		documentClient = new FakeDocumentClient(new Map(), tableName, keySchema);
		documentClient.set(marshaledEntity);
		documentClient.set({id: "second", flag: 20, a: {b: 3, c: 4}});
		documentClient.set({id: "third", flag: 30, a: {b: 5, c: 6}});
		entityManager = new DynamoEntityManager(
			new DynamoDBRepository<IEntity>(
				documentClient as any as DocumentClient,
				tableName,
				keySchema,
				unMarshal,
			),
			documentClient as any as DocumentClient,
			tableName,
			marshal,
		);
		entityManager.waitTimeBetweenTries = 0;
		entity = await entityManager.get({id: entityId});
	});

	describe("when updated a entity", () => {
		const added = 5;
		beforeEach(() => entity.flag += added);
		describe("and flushed", () => {
			beforeEach(() => entityManager.flush());
			it("should update the item in the collection", async () => {
				const item = await documentClient.getByKey({id: entityId});
				expect(item.flag).to.be.eq(originalFlag + added);
			});
		});
	});
	describe("when updated a entity and the updated entity is same as original", () => {
		it("should not update the item in the collection", async () => {
			documentClient.failOnCall();
			entity.a = {b: 1, c: 2};
			await entityManager.flush();
		});
	});
	describe("when persisting a new entity", () => {
		const flag = 40;
		const newId = "newId";
		beforeEach(() => entityManager.persist({id: newId, flag}));
		describe("and flushed", () => {
			beforeEach(() => entityManager.flush());
			it("should save the item in the collection", async () => {
				const item = await documentClient.getByKey({id: newId});
				expect(item.flag).to.be.eq(marshalFlag(flag));
			});
			it("should marshal the item", async () => {
				const item = await documentClient.getByKey({id: newId});
				expect(item.flag).to.be.eq(marshalFlag(flag));
			});
		});
	});
});
