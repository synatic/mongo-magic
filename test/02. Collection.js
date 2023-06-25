const assert = require('assert');
const moment = require('moment');
const stream = require('stream');

const {MongoClient} = require('mongodb');
const Collection = require('../lib').Collection;
const MongoQuery = require('../lib').MongoQuery;
const {EJSON} = require('bson');

const config = {
    databaseName: 'mongo-magic-tests',
    connectionString: 'mongodb://localhost:27017',
    connectionOptions: {},
};

let client = null;
let _db = null;

describe('Collection', function () {
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

    it('should throw an error when no config specified', function (done) {
        assert.throws(
            function () {
                new Collection();
            },
            Error,
            'No error thrown'
        );
        done();
    });

    describe('Query', function () {
        before(async function () {
            await _db.collection('testquery').insertMany(
                [
                    {
                        val: 'a',
                    },
                    {
                        val: 'b',
                    },
                ]);
        });

        it('should count the documents - old', function (done) {
            const collection = new Collection(_db.collection('testquery'));

            const mQuery = new MongoQuery({});
            collection.count(mQuery, function (err, count) {
                assert(!err, 'Error Occurred');
                assert.strictEqual(count, 2, 'Invalid count');
                done();
            });
        });

        it('should count the documents', function (done) {
            const collection = new Collection(_db.collection('testquery'));

            const mQuery = new MongoQuery({});
            collection.countDocuments(mQuery, function (err, count) {
                assert(!err, 'Error Occurred');
                assert.strictEqual(count, 2, 'Invalid count');
                done();
            });
        });

        it('should query document with projection', function (done) {
            const collection = new Collection(_db.collection('testquery'));

            const mQuery = new MongoQuery({$projection: {val: 1, val2: {$concat: ['$val', '_t']}, _id: 0}});

            collection.query(mQuery, (err, results) => {
                assert(!err, 'Has error');
                assert.deepStrictEqual(
                    results,
                    [
                        {
                            val: 'a',
                            val2: 'a_t',
                        },
                        {
                            val: 'b',
                            val2: 'b_t',
                        },
                    ],
                    'invalid projection'
                );
                done();
            });
        });

        it('should stream results', function (done) {
            const collection = new Collection(_db.collection('testquery'));
            const ws = new stream.Writable({objectMode: true});
            let writeCnt = 0;
            ws._write = function (chunk, encoding, done) {
                writeCnt++;
                return done();
            };

            ws.on('finish', function () {
                assert.strictEqual(writeCnt, 2, 'Invalid writes');
                return done();
            });

            const mQuery = new MongoQuery({$limit: 1000});
            collection.queryAsStream(mQuery).pipe(ws);
        });

        it('should stream and transform results', function (done) {
            const collection = new Collection(_db.collection('testquery'));
            const ws = new stream.Writable({objectMode: true});
            let writeCnt = 0;
            ws._write = function (chunk, encoding, done) {
                writeCnt++;
                return done();
            };

            ws.on('finish', function () {
                assert.strictEqual(writeCnt, 2, 'Invalid writes');
                return done();
            });

            const mQuery = new MongoQuery({limit: 1000});
            collection.queryAsStream(mQuery, {transform: (x) => EJSON.serialize(x, {})}).pipe(ws);
        });
    });

    describe('Stats', function () {
        before(async function () {
            await _db.collection('teststats').insertMany(
                [
                    {
                        val: 'a',
                    },
                    {
                        val: 'b',
                    },
                ]);
        });

        it('should throw an error on invalid config 1', function (done) {
            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats({}, function (err) {
                assert(err, 'no error');
                done();
            });
        });

        it('should throw an error on invalid config 2', function (done) {
            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats({statsField: '123'}, function (err) {
                assert(err, 'no error');
                done();
            });
        });

        it('should update stats', function (done) {
            const date = new Date('2022-05-04');
            const momentDate = moment(date).utc();
            const year = momentDate.year();
            const month = (momentDate.month() + 1).toString().padStart(2, '0');
            const day = momentDate.date().toString().padStart(2, '0');
            const hour = momentDate.hour().toString().padStart(2, '0');

            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats(
                {
                    statsField: 'stats1',
                    date: momentDate.toDate(),
                    query: {val: 'a'},
                    increments: {
                        field: 'counter',
                        value: 1,
                    },
                },
                function (err) {
                    assert(!err, 'Error Occurred');
                    _db.collection('teststats').findOne({val: 'a'}).then(function (result) {
                        assert.strictEqual(result.stats1.counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter, 1, 'Invalid stats value');
                        return done();
                    }).catch(done);
                }
            );
        });

        it('should update stats at a different date', function (done) {
            const date = new Date('2022-05-04');
            const momentDate = moment(date).utc().subtract(1, 'month');
            const year = momentDate.year();
            const month = (momentDate.month() + 1).toString().padStart(2, '0');
            const day = momentDate.date().toString().padStart(2, '0');
            const hour = momentDate.hour().toString().padStart(2, '0');

            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats(
                {
                    statsField: 'stats1',
                    date: momentDate.toDate(),
                    query: {val: 'a'},
                    increments: {
                        field: 'counter',
                        value: 10,
                    },
                },
                function (err) {
                    assert(!err, 'Error Occurred');
                    _db.collection('teststats').findOne({val: 'a'}).then( function (result) {
                        assert.strictEqual(result.stats1.counter, 11, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter, 11, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter, 10, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter, 10, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter, 10, 'Invalid stats value');
                        return done();
                    }).catch(done);
                }
            );
        });

        it('should update multiple stats', function (done) {
            const date = new Date();
            const momentDate = moment(date).utc();
            const year = momentDate.year();
            const month = (momentDate.month() + 1).toString().padStart(2, '0');
            const day = momentDate.date().toString().padStart(2, '0');
            const hour = momentDate.hour().toString().padStart(2, '0');

            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats(
                {
                    statsField: 'stats1',
                    date: new Date(),
                    query: {val: 'b'},
                    increments: [
                        {
                            field: 'counter1',
                            value: -1,
                        },
                        {
                            field: 'counter2',
                            value: 1,
                        },
                    ],
                },
                function (err) {
                    assert(!err, 'Error Occurred');
                    _db.collection('teststats').findOne({val: 'b'}).then(function (result) {
                        assert.strictEqual(result.stats1.counter1, -1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter1, -1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter1, -1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter1, -1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter1, -1, 'Invalid stats value');
                        assert.strictEqual(result.stats1.counter2, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter2, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter2, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter2, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter2, 1, 'Invalid stats value');
                        return done();
                    }).catch(done);
                }
            );
        });
    });
});
