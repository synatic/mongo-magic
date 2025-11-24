const assert = require('assert');
const {MongoClient, ObjectId} = require('mongodb');
const MongoQuery = require('../lib').MongoQuery;

const config = {
    databaseName: 'mongo-magic-tests',
    connectionString: 'mongodb://localhost:27017',
    connectionOptions: {},
};

let client = null;
let _db = null;

describe('Mongo Query', function () {
    this.timeout(30000);
    before(async function () {
        client = await MongoClient.connect(config.connectionString, config.connectionOptions);

        _db = client.db(config.databaseName);

        const collections = await _db.collections();

        for (const collection of collections) {
            await collection.deleteMany({});
        }
    });

    after(async function () {
        if (client) {
            await client.close();
        }
    });

    describe('Query', function () {
        it('should throw an error when no config specified', function (done) {
            assert.throws(
                function () {
                    new MongoQuery();
                },
                Error,
                'No error thrown'
            );
            done();
        });

        it('should parse a simple query string', function (done) {
            const mongoQuery = new MongoQuery('$sort=-field1,field2&$select=field1,field2');

            assert.deepStrictEqual(mongoQuery.parsedQuery.select, {field1: true, field2: true}, 'Invalid Select');
            assert.deepStrictEqual(mongoQuery.parsedQuery.sort, {field1: -1, field2: 1}, 'Invalid Sort');
            done();
        });

        it('should parse a complex select with inclusions', function (done) {
            const mongoQuery = new MongoQuery('$select=field1,field2');

            assert.deepStrictEqual(mongoQuery.parsedQuery.select, {field1: true, field2: true}, 'Invalid Select');
            done();
        });

        it('should parse a projection', function (done) {
            const mongoQuery = new MongoQuery('$projection={"field1":1,"field2":1}');

            assert.deepStrictEqual(mongoQuery.parsedQuery.projection, {field1: 1, field2: 1}, 'Invalid Select');
            done();
        });

        it('should parse a complex select with exclusions', function (done) {
            const mongoQuery = new MongoQuery('$select=-field1,-field2');

            assert.deepStrictEqual(mongoQuery.parsedQuery.select, {field1: false, field2: false}, 'Invalid Select');
            done();
        });

        it('should throw an error on a select with exclusions and inclusions', function (done) {
            assert.throws(
                function () {
                    new MongoQuery('$select=field1,-field2');
                },
                Error,
                'No error thrown'
            );
            done();
        });

        it('should throw an error on a select with exclusions and inclusions 2', function (done) {
            assert.throws(
                function () {
                    new MongoQuery('$select=-field1,field2');
                },
                Error,
                'No error thrown'
            );
            done();
        });

        it('should parse a complex filter', function (done) {
            const mongoQuery = new MongoQuery("$filter=field1/field2 eq 'a'");

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': 'a'}, 'Invalid Select');
            done();
        });

        it('should parse a raw query', function (done) {
            const mongoQuery = new MongoQuery('$rawQuery={"field1.field2":{"$date":"2016-01-01T00:00:00Z"}}');

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': new Date('2016-01-01T00:00:00Z')}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with array', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {$or: [{'field1.field2': {$date: '2016-01-01T00:00:00Z'}}]}});

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {$or: [{'field1.field2': new Date('2016-01-01T00:00:00Z')}]},
                'Invalid Raw Query'
            );
            done();
        });

        it('should parse a raw query with int', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$int: '1'}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': 1}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with bool', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$bool: '1'}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': true}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with bool 2', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$bool: 'false'}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': false}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with int in array', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {field: {$in: [{$int: '1'}, {$int: '2'}]}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {field: {$in: [1, 2]}}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with regularExpression in \'$in\' array', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {
                field: {$in: [
                    {$regularExpression: { pattern: ';', options: 'i' }},
                    {$regularExpression: { pattern: '/', options: 'i' }}
                ]}}
            });

            assert(mongoQuery.parsedQuery.query.field.$in[0] instanceof RegExp, 'Invalid parsed $in RegExp query');
            assert(mongoQuery.parsedQuery.query.field.$in[1] instanceof RegExp, 'Invalid parsed $in RegExp query');
            done();
        });

        it('should parse a raw query with int', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$float: '1.2'}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': 1.2}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with date', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {$and: [{_dateUpdated: {$gt: {$date: '2020-01-06'}}}]}});

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {
                    $and: [
                        {
                            _dateUpdated: {
                                $gt: new Date('2020-01-06T00:00:00.000Z'),
                            },
                        },
                    ],
                },
                'Invalid Raw Query'
            );
            done();
        });

        it('should parse a raw query with objectId string', function (done) {
            const objectId = new ObjectId();
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$objectId: objectId.toString()}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': objectId}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with objectId object', function (done) {
            const objectId = new ObjectId();
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$objectId: objectId}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': objectId}, 'Invalid Raw Query');
            done();
        });

        it('should throw an error on a raw query with invalid objectId', function (done) {
            assert.throws(
                function () {
                    new MongoQuery({$rawQuery: {'field1.field2': {$objectId: 'invalid-object-id'}}});
                },
                Error,
                'No error thrown'
            );
            done();
        });

        it('should parse a raw query with string', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$string: 123}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': '123'}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with and null', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$string: null}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': null}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with string and boolean', function (done) {
            const mongoQuery = new MongoQuery({$rawQuery: {'field1.field2': {$string: false}}});

            assert.deepStrictEqual(mongoQuery.parsedQuery.query, {'field1.field2': 'false'}, 'Invalid Raw Query');
            done();
        });

        it('should parse a raw query with array and qs', function (done) {
            const mongoQuery = new MongoQuery('$rawQuery={"$or":[{"field1.field2":{"$date":"2016-01-01T00:00:00Z"}}]}');

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {$or: [{'field1.field2': new Date('2016-01-01T00:00:00Z')}]},
                'Invalid Raw Query'
            );
            done();
        });

        it('should parse a raw query and filter', function (done) {
            const mongoQuery = new MongoQuery('$filter=field2 eq \'a\'&$rawQuery={"field1":{"$date":"2016-01-01T00:00:00Z"}}');

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {
                    field1: new Date('2016-01-01T00:00:00Z'),
                    field2: 'a',
                },
                'Invalid Query'
            );
            done();
        });

        it('should parse a raw query and filter with a default', function (done) {
            const mongoQuery = new MongoQuery('$filter=field2 eq \'a\'&$rawQuery={"field1":{"$date":"2016-01-01T00:00:00Z"}}', {field3: 3});

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {
                    field1: new Date('2016-01-01T00:00:00Z'),
                    field2: 'a',
                    field3: 3,
                },
                'Invalid Query'
            );
            done();
        });

        it('should parse a raw query and filter with a mongo id', function (done) {
            const objectId = new ObjectId();
            const mongoQuery = new MongoQuery('$filter=field2 eq \'a\'&$rawQuery={"field1":{"$date":"2016-01-01T00:00:00Z"}}', {
                field3: objectId,
            });

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {
                    field1: new Date('2016-01-01T00:00:00Z'),
                    field2: 'a',
                    field3: objectId,
                },
                'Invalid Query'
            );
            done();
        });

        it('should parse a raw query and filter with a array', function (done) {
            const objectId = new ObjectId();
            const mongoQuery = new MongoQuery('$rawQuery={"$and":[{"field2":"x"}],"field1":{"$date":"2016-01-01T00:00:00Z"}}', {
                field3: objectId,
                $and: [{field4: 'y'}],
            });

            assert.deepStrictEqual(
                mongoQuery.parsedQuery.query,
                {
                    field1: new Date('2016-01-01T00:00:00Z'),
                    $and: [
                        {
                            field2: 'x',
                            field4: 'y',
                        },
                    ],
                    field3: objectId,
                },
                'Invalid Query'
            );
            done();
        });
    });

    describe('Merge', function () {
        it('should merge a query with no from and to', function () {
            assert.equal(MongoQuery.mergeQuery(), null, 'Invalid parse');
        });

        it('should merge a query with no from', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery({a: 1}), {a: 1}, 'Invalid parse');
        });

        it('should merge a query with no to', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery(null, {a: 2}), {a: 2}, 'Invalid parse');
        });

        it('should merge a query', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery({a: 1}, {a: 2}), {$and: [{a: 1}, {a: 2}]}, 'Invalid parse');
        });

        it('should merge a query with string', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery({a: 1}, '{"a":2}'), {$and: [{a: 1}, {a: 2}]}, 'Invalid parse');
        });

        it('should merge a query', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery({a: 2}, {$and: [{a: 1}]}), {$and: [{a: 1}, {a: 2}]}, 'Invalid parse');
        });

        it('should merge a query with or', function () {
            assert.deepStrictEqual(MongoQuery.mergeQuery({a: 1}, {a: 2}, 'or'), {$or: [{a: 1}, {a: 2}]}, 'Invalid parse');
        });
    });
});
