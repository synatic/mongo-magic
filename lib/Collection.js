const $moment = require('moment');
const _s = require('underscore.string');
const $check = require('check-types');
const MongoQuery = require('./MongoQuery.js');

const _defaultQueryTimeoutMS = 120000;
// const _specialFields = ['$select', '$limit', '$top', '$filter', '$skip', '$sort', '$orderby', '$rawQuery', '$aggregate', '$group'];

class Collection {
    constructor(collection, options) {
        if (!collection) throw new Error('Missing Collection');
        this.options = options ? options : {};
        if (!this.options.queryTimeout) this.options.queryTimeout = _defaultQueryTimeoutMS;
        this.collection = collection;
    }

    count(query, callback) {
        if (!$check.instanceStrict(query, MongoQuery)) return callback('Invalid query object');

        const countRetrieved = (err, count) => {
            if (err) return callback(err);
            if (!count) return callback(null, 0);
            else return callback(null, count);
        };

        try {
            if (this.options.estimatedDocumentCount && this.collection.estimatedDocumentCount) {
                this.collection.estimatedDocumentCount(query.parsedQuery.query, countRetrieved);
            } else if (this.collection.countDocuments) {
                this.collection.countDocuments(query.parsedQuery.query, countRetrieved);
            } else {
                // todo: remove this once we've completed the mongodb package updates across all systems
                this.collection.count(query.parsedQuery.query, countRetrieved);
            }
        } catch (exp) {
            return callback(exp);
        }
    }

    queryAsCursor(query) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');

        const cur = this.collection.find(query.parsedQuery.query);

        // top and limit
        cur.limit(query.parsedQuery.limit);

        // select
        if (query.parsedQuery.select) cur.project(query.parsedQuery.select);

        // sort
        if (query.parsedQuery.sort) cur.sort(query.parsedQuery.sort);

        // orderby
        if (query.parsedQuery.orderby) cur.sort(query.parsedQuery.orderby);

        // skip
        if (query.parsedQuery.skip) cur.skip(query.parsedQuery.skip);
        else cur.skip(0);

        cur.maxTimeMS(this.options.queryTimeout);

        return cur;
    }

    query(query, callback) {
        if (!$check.instanceStrict(query, MongoQuery)) return callback('Invalid query object');

        try {
            const cur = this.queryAsCursor(query);

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

    queryAsStream(query, serializeOption) {
        if (!$check.instanceStrict(query, MongoQuery)) throw new Error('Invalid query object');
        const cur = this.queryAsCursor(query);

        if(serializeOption === 'EJSON')
            return cur.stream({transform: x => EJSON.serialize(x, {})});

        return cur.stream();
    }

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
        const monthPath = yearPath + '.' + _s.lpad(processingDate.month() + 1, 2, '0');
        const dayPath = monthPath + '.' + _s.lpad(processingDate.date(), 2, '0');
        const hourPath = dayPath + '.' + _s.lpad(processingDate.hour(), 2, '0');

        for (const increment of incrementFields) {
            update.$inc[basePath + increment.field] = increment.value;
            update.$inc[yearPath + '.' + increment.field] = increment.value;
            update.$inc[monthPath + '.' + increment.field] = increment.value;
            update.$inc[dayPath + '.' + increment.field] = increment.value;
            update.$inc[hourPath + '.' + increment.field] = increment.value;
        }
        this.collection.updateOne(options.query, update, callback);
    }
}

module.exports = Collection;
