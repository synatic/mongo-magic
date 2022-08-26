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
 * @typedef {import('mongodb').FindOptions} FindOptions
 * @typedef {import('mongodb').FindCursor} FindCursor
 * @typedef {import('mongodb').CursorStreamOptions} CursorStreamOptions
 * @typedef {import('stream').Readable} Readable
 * @typedef {import('mongodb').Collection} Collection
 */

/** A wrapper around the mongodb collection with some helper functions
 * @class Collection
 */
class Collection {
    /** Creates an instance of Collection.
     *
     * @param {Collection} collection - the underlying mongo collection
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

    /** Counts the documents in the collection
     * @param {Object} query
     * @param {EstimatedDocumentCountOptions | CountDocumentsOptions} [options={}]
     * @param {Function} callback
     * @returns {*}
     */
    count(query, options = {}, callback) {
        if ($check.function(options)) {
            callback = options;
            options = {};
        } else {
            options = options || {};
        }
        if (!$check.instanceStrict(query, MongoQuery)) return callback('Invalid query object');

        const countRetrieved = (err, count) => {
            if (err) return callback(err);
            if (!count) return callback(null, 0);
            else return callback(null, count);
        };

        const filter = query.parsedQuery.query || {};
        try {
            if (this._options.estimatedDocumentCount && this._collection.estimatedDocumentCount) {
                this._collection.estimatedDocumentCount(options, countRetrieved);
            } else if (this._collection.countDocuments) {
                this._collection.countDocuments(filter, options, countRetrieved);
            } else {
                // todo: remove this once we've completed the mongodb package updates across all systems
                this._collection.count(filter, options, countRetrieved);
            }
        } catch (exp) {
            return callback(exp);
        }
    }

    /** Creates a cursor given the MongoQuery Object
     * @param {Object} query - the MongoQuery Object
     * @param {FindOptions} [options={}]
     * @returns {FindCursor}
     */
    queryAsCursor(query, options = {}) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');

        const cur = this._collection.find(query.parsedQuery.query, options);

        // top and limit
        cur.limit(query.parsedQuery.limit);

        // select
        if (query.parsedQuery.select) cur.project(query.parsedQuery.select);
        if (query.parsedQuery.projection) cur.project(query.parsedQuery.projection);

        // sort
        if (query.parsedQuery.sort) cur.sort(query.parsedQuery.sort);

        // orderby
        if (query.parsedQuery.orderby) cur.sort(query.parsedQuery.orderby);

        // skip
        if (query.parsedQuery.skip) cur.skip(query.parsedQuery.skip);
        else cur.skip(0);

        if ($check.assigned(options.maxTimeMS)) {
            cur.maxTimeMS(options.maxTimeMS);
        } else {
            cur.maxTimeMS(this._options.queryTimeout);
        }

        return cur;
    }

    /** Executes the MongoQuery as an Array
     * @param {Object} query - the MongoQuery Object
     * @param {FindOptions} [options={}]
     * @param {Function} callback
     * @returns {*}
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
            const cur = this.queryAsCursor(query, options);

            cur.toArray(function (err, results) {
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

    /** Executes the MongoQuery as a stream
     * @param {Object} query - the MongoQuery Object
     * @param {FindOptions} [options={}]
     * @param {CursorStreamOptions} [streamOptions={}]
     * @returns {Readable}
     * @memberof Collection
     */
    queryAsStream(query, options = {}, streamOptions = {}) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');
        const cur = this.queryAsCursor(query, options);

        return cur.stream(streamOptions);
    }

    /**
     * @param {Object} options
     * @param {Function} callback
     * @returns {void}
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
        this._collection.updateOne(options.query, update, callback);
    }

    /** Gets the underlying MongoDB collection
     *
     * @return {*}
     */
    get collection() {
        return this._collection;
    }
}

module.exports = Collection;
