/* eslint-disable valid-jsdoc */

const {callbackify} = require('node:util');
const $moment = require('moment');
const $check = require('check-types');
const MongoQuery = require('./MongoQuery.js');

const _defaultQueryTimeoutMS = 120000;

// const _specialFields = ['$select', '$limit', '$top', '$filter', '$skip', '$sort', '$orderby', '$rawQuery', '$aggregate', '$group'];
/**
 * @typedef {import('mongodb').EstimatedDocumentCountOptions} EstimatedDocumentCountOptions
 * @typedef {import('mongodb').CountDocumentsOptions} CountDocumentsOptions
 * @typedef {import('mongodb').CountOptions} CountOptions
 * @typedef {import('mongodb').CursorStreamOptions} CursorStreamOptions
 * @typedef {import('stream').Readable} Readable
 */

/** A wrapper around the mongodb collection with some helper functions
 * @class Collection
 * @template {Document} TSchema
 */
class Collection {
    /** Creates an instance of Collection.
     *
     * @param {import('mongodb').Collection<TSchema>} collection - the underlying mongo collection
     * @param {object} [options] - the query options
     * @param {number} [options.queryTimeout] - the query timeout defaults to 120000ms
     * @param {boolean} [options.estimatedDocumentCount] - use estimated document count when calling count
     */
    constructor(collection, options) {
        if (!collection) throw new Error('Missing Collection');
        this._options = options || {};
        if (!this._options.queryTimeout) this._options.queryTimeout = _defaultQueryTimeoutMS;
        this._collection = collection;
    }

    // todo: remove once
    /**
     * Counts the documents in the collection
     * @deprecated in favour of using countDocuments
     * @param {MongoQuery<TSchema>} query
     * @param {EstimatedDocumentCountOptions | CountDocumentsOptions} [options={}]
     * @param {CountDocumentsCallback} callback
     * @return {*}
     */
    count(query, options = {}, callback) {
        this.countDocuments(query, options, callback);
    }

    /**
     * @callback CountDocumentsCallback
     * @param {Error|null} [err]
     * @param {number} [count]
     *
     * Counts the documents in the collection
     * @param {MongoQuery<TSchema>} query
     * @param {EstimatedDocumentCountOptions | CountDocumentsOptions|CountDocumentsCallback} [options={}]
     * @param {CountDocumentsCallback} callback
     * @return {*}
     */
    countDocuments(query, options = {}, callback) {
        if ($check.function(options)) {
            callback = options;
            options = {};
        } else {
            options = options || {};
        }
        if (!$check.instanceStrict(query, MongoQuery)) {
            return callback(new Error('Invalid query object'));
        }

        const filter = query.parsedQuery.query || {};
        callbackify(() => {
            return this._collection.countDocuments(filter, options);
        })((err, count) => {
            if (err) {
                return callback(err);
            }

            if (!count) {
                return callback(null, 0);
            }

            return callback(null, count);
        });
    }

    /**
     * Creates a cursor given the MongoQuery Object
     * @param {MongoQuery<TSchema>} query - the MongoQuery Object
     * @param {import('mongodb').FindOptions<TSchema>} [options={}]
     * @return {import('mongodb').FindCursor<TSchema>}
     */
    queryAsCursor(query, options = {}) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');

        const filter = query.parsedQuery.query || {};
        const cursor = this._collection.find(filter, options);

        // top and limit
        cursor.limit(query.parsedQuery.limit);

        // select
        if (query.parsedQuery.select) cursor.project(query.parsedQuery.select);
        if (query.parsedQuery.projection) cursor.project(query.parsedQuery.projection);

        // sort
        if (query.parsedQuery.sort) cursor.sort(query.parsedQuery.sort);

        // orderby
        if (query.parsedQuery.orderby) cursor.sort(query.parsedQuery.orderby);

        // skip
        if (query.parsedQuery.skip) cursor.skip(query.parsedQuery.skip);
        else cursor.skip(0);

        if ($check.assigned(options.maxTimeMS)) {
            cursor.maxTimeMS(options.maxTimeMS);
        } else {
            cursor.maxTimeMS(this._options.queryTimeout);
        }

        return cursor;
    }

    /**
     * @callback QueryCallback
     * @param {Error|null} [err]
     * @param {TSchema[]} [results]
     */
    /**
     * Executes the MongoQuery as an Array
     * @param {MongoQuery<TSchema>} query - the MongoQuery Object
     * @param {import('mongodb').FindOptions<TSchema>} [options={}]
     * @param {QueryCallback} callback
     * @return {*}
     */
    query(query, options = {}, callback) {
        if ($check.function(options)) {
            callback = options;
            options = {};
        } else {
            options = options || {};
        }
        if (!$check.instanceStrict(query, MongoQuery)) {
            return callback(new Error('Invalid query object'));
        }
        try {
            callbackify(() => {
                return this.queryAsCursor(query, options).toArray();
            })((err, results) => {
                if (err) {
                    return callback(err);
                }

                if (!results) {
                    return callback(null, []);
                }
                return callback(null, results);
            });
        } catch (exp) {
            return callback(exp);
        }
    }

    /**
     * Executes the MongoQuery as a stream
     * @param {MongoQuery<TSchema>} query - the MongoQuery Object
     * @param {import('mongodb').FindOptions} [options={}]
     * @param {CursorStreamOptions} [streamOptions={}]
     * @return {import('stream').Readable}
     * @memberof Collection
     */
    queryAsStream(query, options = {}, streamOptions = {}) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');
        const cursor = this.queryAsCursor(query, options);

        return cursor.stream(streamOptions);
    }

    /**
     * @callback UpdateStatsCallback
     * @param {Error|null} [err]
     * @param {import('mongodb'.UpdateResult<TSchema>} [result]
     * //
     * @param {import('./types').UpdateStatsOptions<TSchema>} options
     * @param {function} callback
     * @return {UpdateStatsCallback}
     * @memberof Collection
     */
    updateStats(options, callback) {
        if (!options.statsField) {
            return callback(new Error('Missing stats field'));
        }
        if (!options.increments) {
            return callback(new Error('Missing increments field'));
        }
        if (!options.date) {
            return callback(new Error('Missing date field'));
        }
        if (!options.query) {
            return callback(new Error('Missing query'));
        }

        let incrementFields = null;
        if ($check.array(options.increments)) {
            incrementFields = options.increments;
        } else if ($check.object(options.increments)) {
            incrementFields = [options.increments];
        } else {
            throw new Error('Invalid increments');
        }

        const processingDate = $moment(options.date).utc();

        const update = {$inc: {}};
        const basePath = options.statsField + '.';
        const yearPath = basePath + processingDate.year();
        const monthPath = yearPath + '.' + (processingDate.month() + 1).toString().padStart(2, '0');
        const dayPath = monthPath + '.' + processingDate.date().toString().padStart(2, '0');
        const hourPath = dayPath + '.' + processingDate.hour().toString().padStart(2, '0');

        for (const increment of incrementFields) {
            update.$inc[basePath + increment.field] = increment.value;
            update.$inc[yearPath + '.' + increment.field] = increment.value;
            update.$inc[monthPath + '.' + increment.field] = increment.value;
            update.$inc[dayPath + '.' + increment.field] = increment.value;
            update.$inc[hourPath + '.' + increment.field] = increment.value;
        }
        callbackify(() => {
            return this._collection.updateOne(options.query, update);
        })((err, result) => {
            if (err) {
                return callback(err);
            }

            return callback(null, result);
        });
    }

    /**
     * Gets the underlying MongoDB collection
     * @return {import('mongodb').Collection<TSchema>}
     */
    get collection() {
        return this._collection;
    }
}

module.exports = Collection;
