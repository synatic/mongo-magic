const {ObjectId} = require('mongodb');
const $check = require('check-types');
const $json = require('@synatic/json-magic');

const _unsafeAggregateFunctions = [
    {name: '$collStats', aggregate: true},
    {name: '$currentOp', aggregate: true},
    {name: '$indexStats', aggregate: true},
    {name: '$listLocalSessions', aggregate: true},
    {name: '$listSessions', aggregate: true},
    {name: '$merge', aggregate: true},
    {name: '$out', aggregate: true},
    {name: '$planCacheStats', aggregate: true},
    {name: '$explain', aggregate: false},
    {name: '$hint', aggregate: false},
    {name: '$showDiskLoc', aggregate: false},
    {name: '$where', aggregate: false},
];

/**
 *Provides utilities for mongodb
 */
class Utils {
    /** Generates a new MongoID
     *
     * @return {ObjectId}
     */
    static generateId() {
        return new ObjectId();
    }

    /** Checks whether a provided id is a valid mongo id
     *
     * @param {string|object} id - the mongo id to validate
     * @return {boolean}
     */
    static isValidId(id) {
        if (!id) {
            return false;
        }

        if ($check.object(id) && id._bsontype === 'ObjectId' && Buffer.isBuffer(id.id) && id.id.length === 12) {
            return ObjectId.isValid(id.id);
        }

        if (id.toString().length !== 24) {
            return false;
        }

        return ObjectId.isValid(id.toString());
    }

    /** Parses an id to a mongodb ObjectId
     *
     * @param {string|object} id - the mongo id to validate
     * @return {null|ObjectId|boolean}
     */
    static parseId(id) {
        if (!id) {
            return false;
        }

        if ($check.object(id) && id._bsontype === 'ObjectId' && Buffer.isBuffer(id.id) && id.id.length === 12) {
            return new ObjectId(id.id);
        }

        if (id.toString().length !== 24) {
            return null;
        }

        if (ObjectId.isValid(id.toString())) {
            return new ObjectId(id.toString());
        }
    }

    /** Retrieves the collection paths used in aggregates by lookups
     *
     * @param {object[]} aggregate - the aggregate to get paths for
     * @return {object[]} - the paths nad collection names
     * @throws
     */
    static getCollectionPathsForAggregate(aggregate) {
        if (!$check.array) {
            throw new Error('Invalid aggregate array');
        }

        const collectionPaths = [];
        $json.walk(aggregate, (val, path) => {
            if (
                path.endsWith('$lookup/from') ||
                path.endsWith('$graphLookup/from') ||
                path.endsWith('$unionWith/coll') ||
                path.endsWith('$unionWith')
            ) {
                collectionPaths.push({
                    path: path,
                    collection: val,
                });
            }
        });

        return collectionPaths;
    }

    /** Cleans a aggregate of any system functions and throws errors if used in queries
     *
     * @param {object[]} aggregate - the aggregate to get paths for
     * @param {boolean} throwErrorOnUnsafe - Specifies if the library should throw an error when the aggregation pipeline is unsafe
     * @return {object[]} - the paths nad collection names
     * @throws
     */
    static cleanAggregate(aggregate, throwErrorOnUnsafe = true) {
        if (!$check.array(aggregate)) {
            throw new Error('Invalid aggregate array');
        }

        const checkUnsafe = (pipeline) => {
            if (!$check.array(pipeline)) {
                throw new Error('Invalid aggregate array');
            }
            const newAggr = [];
            for (const agg of pipeline) {
                let unsafe = false;
                for (const invalidAggr of _unsafeAggregateFunctions.filter((a) => a.aggregate)) {
                    if (agg[invalidAggr.name]) {
                        unsafe = true;
                        if (throwErrorOnUnsafe) {
                            throw new Error(`Unsafe function "${invalidAggr.name}" in pipeline:\n${JSON.stringify(pipeline, null, 4)}`);
                        }
                        break;
                    }
                }
                if (!unsafe) {
                    if (agg.$lookup && agg.$lookup.pipeline) {
                        agg.$lookup.pipeline = checkUnsafe(agg.$lookup.pipeline);
                    }
                    if (agg.$unionWith && agg.$unionWith.pipeline) {
                        agg.$unionWith.pipeline = checkUnsafe(agg.$unionWith.pipeline);
                    }

                    newAggr.push(agg);
                }
            }
            return newAggr;
        };

        const newAggr = checkUnsafe(aggregate);

        let unsafe = null;
        $json.walk(newAggr, (val, path) => {
            for (const invalidAggr of _unsafeAggregateFunctions) {
                if (path.endsWith('/' + invalidAggr.name) || path.indexOf('/' + invalidAggr.name + '/') > -1) {
                    unsafe = path;
                    break;
                }
            }
        });
        if (unsafe) {
            throw new Error(`Unsafe functions at path: ${unsafe}\nPipeline:\n${JSON.stringify(newAggr, null, 4)}`);
        }

        return newAggr;
    }
}

module.exports = Utils;
