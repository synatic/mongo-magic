/* eslint-disable valid-jsdoc */

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
 */
class Collection {
    /** Creates an instance of Collection.
     *
     * @param {import('mongodb').Collection} collection - the underlying mongo collection
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
     * @param {Object} query
     * @param {EstimatedDocumentCountOptions | CountDocumentsOptions} [options={}]
     * @param {Function} callback
     * @return {*}
     */
    count(query, options = {}, callback) {
        this.countDocuments(query, options, callback);
    }

    /**
     * Counts the documents in the collection
     * @param {Object} query
     * @param {EstimatedDocumentCountOptions | CountDocumentsOptions} [options={}]
     * @param {Function} callback
     * @return {*}
     */
    countDocuments(query, options = {}, callback) {
        if ($check.function(options)) {
            callback = options;
            options = {};
        } else {
            options = options || {};
        }
        if (!$check.instanceStrict(query, MongoQuery)) return callback('Invalid query object');

        const filter = query.parsedQuery.query || {};
        this._collection
            .countDocuments(filter, options)
            .then((count) => {
                if (!count) {
                    return callback(null, 0);
                }

                return callback(null, count);
            })
            .catch(callback);
    }

    /**
     * Creates a cursor given the MongoQuery Object
     * @param {Object} query - the MongoQuery Object
     * @param {import('mongodb').FindOptions} [options={}]
     * @return {import('mongodb').FindCursor}
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
     * Executes the MongoQuery as an Array
     * @param {Object} query - the MongoQuery Object
     * @param {import('mongodb').FindOptions} [options={}]
     * @param {function} callback
     * @return {*}
     */
    query(query, options = {}, callback) {
        if ($check.function(options)) {
            callback = options;
            options = {};
        } else {
            options = options || {};
        }
        if (!$check.instanceStrict(query, MongoQuery)) return callback('Invalid query object');
        try {
            const cursor = this.queryAsCursor(query, options);

            cursor.toArray().then((results)=>{
                if (!results) {
                    return callback(null, []);
                }
                return callback(null, results);
            }).catch((err) => {
                return callback(err);
            });
        } catch (exp) {
            return callback(exp);
        }
    }

    /**
     * Executes the MongoQuery as a stream
     * @param {Object} query - the MongoQuery Object
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
     * @param {Object} options
     * @param {function} callback
     * @return {function}
     * @memberof Collection
     */
    updateStats(options, callback) {
        if (!options.statsField) return callback('Missing stats field');
        if (!options.increments) return callback('Missing increments field');
        if (!options.date) return callback('Missing date field');
        if (!options.query) return callback('Missing query');

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
        this._collection.updateOne(options.query, update).then((result) => {
            return callback(null, result);
        }).catch(callback);
    }

    /**
     * Gets the underlying MongoDB collection
     * @return {import('mongodb').Collection}
     */
    get collection() {
        return this._collection;
    }
}

module.exports = Collection;
