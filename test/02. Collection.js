const assert = require('assert');
const async = require('async');
const moment = require('moment');
const stream = require('stream');
const _s = require('underscore.string');
const {MongoClient} = require('mongodb');
const Collection = require('../lib').Collection;
const MongoQuery = require('../lib').MongoQuery;
const {EJSON} = require('bson');

const config = {
    databaseName: 'mongo-magic-tests',
    connectionString: 'mongodb://localhost:27017',
    connectionOptions: {
        useUnifiedTopology: true,
        useNewUrlParser: true,
    },
};

let client = null;
let _db = null;

describe('Collection', function () {
    this.timeout(30000);
    before(function (done) {
        async.waterfall(
            [
                (cb) => {
                    client = new MongoClient(config.connectionString, config.connectionOptions);

                    client.connect(function (err) {
                        if (err) {
                            return cb(err);
                        }

                        cb();
                    });
                },
                (cb) => {
                    _db = client.db(config.databaseName);

                    _db.collections(function (err, collections) {
                        if (err) {
                            return cb(err);
                        }

                        async.each(
                            collections,
                            function (item, itemCallback) {
                                item.deleteMany({}, itemCallback);
                            },
                            function (err) {
                                cb(err);
                            }
                        );
                    });
                },
            ],
            function (err) {
                if (err) {
                    console.log(err);
                }
                done(err);
            }
        );
    });

    after(function (done) {
        if (client) {
            client.close();
        }

        done();
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
        before(function (done) {
            _db.collection('testquery').insertMany(
                [
                    {
                        val: 'a',
                    },
                    {
                        val: 'b',
                    },
                ],
                function (err) {
                    return done(err);
                }
            );
        });

        it('should count the documents', function (done) {
            const collection = new Collection(_db.collection('testquery'));

            const mQuery = new MongoQuery({});
            collection.count(mQuery, function (err, count) {
                assert(!err, 'Error Occurred');
                assert.strictEqual(count, 2, 'Invalid count');
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

            const mQuery = new MongoQuery({limit: 1000});
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
            collection.queryAsStream(mQuery, {transform: x => EJSON.serialize(x, {})}).pipe(ws);
        });
    });

    describe('Stats', function () {
        before(function (done) {
            _db.collection('teststats').insertMany(
                [
                    {
                        val: 'a',
                    },
                    {
                        val: 'b',
                    },
                ],
                function (err) {
                    return done(err);
                }
            );
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
            const date = new Date();
            const momentDate = moment(date).utc();
            const year = momentDate.year();
            const month = _s.lpad(momentDate.month() + 1, 2, '0');
            const day = _s.lpad(momentDate.date(), 2, '0');
            const hour = _s.lpad(momentDate.hour(), 2, '0');

            const collection = new Collection(_db.collection('teststats'));
            collection.updateStats(
                {
                    statsField: 'stats1',
                    date: new Date(),
                    query: {val: 'a'},
                    increments: {
                        field: 'counter',
                        value: 1,
                    },
                },
                function (err) {
                    assert(!err, 'Error Occurred');
                    _db.collection('teststats').findOne({val: 'a'}, function (err, result) {
                        assert(!err, 'Error Occurred');
                        assert.strictEqual(result.stats1.counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter, 1, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter, 1, 'Invalid stats value');
                        return done();
                    });
                }
            );
        });

        it('should update stats at a different date', function (done) {
            const date = new Date();
            const momentDate = moment(date).utc().subtract(1, 'month');
            const year = momentDate.year();
            const month = _s.lpad(momentDate.month() + 1, 2, '0');
            const day = _s.lpad(momentDate.date(), 2, '0');
            const hour = _s.lpad(momentDate.hour(), 2, '0');

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
                    _db.collection('teststats').findOne({val: 'a'}, function (err, result) {
                        assert(!err, 'Error Occurred');
                        assert.strictEqual(result.stats1.counter, 11, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year].counter, 11, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month].counter, 10, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day].counter, 10, 'Invalid stats value');
                        assert.strictEqual(result.stats1[year][month][day][hour].counter, 10, 'Invalid stats value');
                        return done();
                    });
                }
            );
        });

        it('should update multiple stats', function (done) {
            const date = new Date();
            const momentDate = moment(date).utc();
            const year = momentDate.year();
            const month = _s.lpad(momentDate.month() + 1, 2, '0');
            const day = _s.lpad(momentDate.date(), 2, '0');
            const hour = _s.lpad(momentDate.hour(), 2, '0');

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
                    _db.collection('teststats').findOne({val: 'b'}, function (err, result) {
                        assert(!err, 'Error Occurred');
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
                    });
                }
            );
        });
    });
});
