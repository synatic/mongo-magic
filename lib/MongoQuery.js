const $merge = require('deepmerge');
const $qs = require('querystring');
const _s = require('underscore.string');
const $check = require('check-types');
const oDataParser = require('./oDataParser.js');
const utils=require('./utils.js')
const _defaultLimit = 50;

class MongoQuery {
    constructor(query, defaults = {}) {
        var originalQuery = query;
        if (!$check.assigned(query)) {
            originalQuery = {};
        }

        if ($check.string(query)) {
            originalQuery = $qs.parse(query);
        } else if ($check.object(query)) {
            originalQuery = query;
        } else {
            throw new Error("Invalid parameter: query");
        }

        this.originalQuery = originalQuery;
        this.parsedQuery = {};
        this.parsedQuery.select = getSelect(originalQuery);
        this.parsedQuery.orderBy = getSortOrOrderBy(originalQuery);
        this.parsedQuery.limit = getTopOrLimit(originalQuery);
        this.parsedQuery.top = this.parsedQuery.limit;
        this.parsedQuery.sort = this.parsedQuery.orderBy;
        this.parsedQuery.skip = getSkip(originalQuery);

        if (originalQuery.$rawQuery) this.parsedQuery.rawQuery = MongoQuery.parseRawQuery(originalQuery.$rawQuery);
        if (originalQuery.$filter) this.parsedQuery.filter = oDataParser.parse(originalQuery.$filter);
        this.parsedQuery.query = getQuery(this.parsedQuery, defaults);
    }

    static parseRawQuery(rawQueryString) {
        if (!rawQueryString) {
            return null;
        }
        let rawQuery = null;

        if (!$check.object(rawQueryString)) {
            try {
                rawQuery = JSON.parse(rawQueryString);
            } catch (exp) {
                throw new Error("Invalid Raw Query String")
            }
        }else{
            rawQuery = rawQueryString;
        }


        const mergeRawQueryRecursive=(obj, parent, parentKey)=> {
            for (let key in obj) {
                if (!obj.hasOwnProperty(key)) continue;

                if ($check.object(obj[key])&&!utils.isValidId(obj[key])){
                    mergeRawQueryRecursive(obj[key], obj, key);
                } else if($check.array(obj[key])){
                    for (let i=0;i<obj[key].length;i++){
                        mergeRawQueryRecursive(obj[key][i], obj[key], i);
                    }
                } else {
                    if (key === "$date") {
                        parent[parentKey] = new Date(obj[key]);
                    } else if (key === "$objectId"&&utils.isValidId(obj[key])) {
                        parent[parentKey] = utils.parseId(obj[key]);
                    }
                    else if (key === "$int") {
                        parent[parentKey] = parseInt(obj[key]);
                    }else if (key === "$float") {
                        parent[parentKey] = parseFloat(obj[key]);
                    }else if (key === "$bool") {
                        let queryVal=obj[key];
                        if (!$check.assigned(obj[key]))queryVal=false;
                        else if (obj[key]===0)queryVal=false;
                        else if (obj[key]===1)queryVal=true;
                        else if (obj[key]===false)queryVal=false;
                        else if (obj[key]===true)queryVal=true;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString()==="0")queryVal=false;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString()==="1")queryVal=true;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString().toLowerCase()==="false")queryVal=false;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString().toLowerCase()==="true")queryVal=true;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString().toLowerCase()==="no")queryVal=false;
                        else if (obj[key]&&obj[key].toString&&obj[key].toString().toLowerCase()==="yes")queryVal=true;

                        parent[parentKey] = queryVal;
                    }
                    else if (key === "$string") {
                        parent[parentKey] = $check.assigned(obj[key])&&obj[key].toString?obj[key].toString():obj[key];
                    }
                }
            }
        };



        mergeRawQueryRecursive(rawQuery);

        return rawQuery;
    }

    /** Merges mongo queries
     *
     * @param {string|object} fromQuery - the query to merge from
     * @param {string|object} toQuery - the query to merge  into
     * @param {string} [type] - the type of merge, and/or. default is and
     * @returns {{$and: [any, *]}|{$and}|*|null}
     */
    static mergeQuery(fromQuery, toQuery, type) {
        if (!fromQuery && !toQuery) {
            return null;
        } else if (!fromQuery) {
            return toQuery;
        } else if (!toQuery) {
            return fromQuery;
        }
        if (!$check.object(fromQuery)) {
            try {
                fromQuery = JSON.parse(fromQuery);
            } catch (exp) {
                throw new Error("Invalid To Query String")
            }
        }

        if (!$check.object(toQuery)) {
            try {
                toQuery = JSON.parse(toQuery);
            } catch (exp) {
                throw new Error("Invalid To Query String")
            }
        }

        if (type !== "or") {
            if (toQuery.$and) {
                toQuery.$and.push(fromQuery);
                return toQuery;
            } else {
                return {
                    $and: [
                        fromQuery,
                        toQuery
                    ]
                }
            }
        } else {
            return {
                $or: [
                    fromQuery,
                    toQuery
                ]
            }
        }


    }

}

function getSelect(query) {
    if (!query.$select) return null;

    let selectedFields = {};
    let hasNegative = false;
    let hasPositive = false;
    let selectFields = query.$select.split(",");
    for (let selectField of selectFields) {
        selectField = selectField.trim();
        if (selectField.substring(0, 1) === "-") {
            selectedFields[selectField.substring(1)] = false;
            hasNegative = true;
        } else {
            selectedFields[selectField] = true;
            hasPositive = true;
        }
    }

    if (hasNegative && hasPositive) throw new Error("Select cannot have inclusion and exclusion together");

    return selectedFields
}

function getSortOrOrderBy(query) {
    if (!query.$orderby && !query.$sort) return null;
    let sortStr = query.$orderby ? query.$orderby : query.$sort;
    return sortStr.split(",").reduce((orderByFields, orderByField) => {
        orderByField = orderByField.trim();
        if (_s.endsWith(orderByField, " desc")) orderByFields[orderByField.replace(" desc", "")] = -1;
        else if (_s.endsWith(orderByField, " asc")) orderByFields[orderByField.replace(" asc", "")] = 1;
        else if (_s.startsWith(orderByField, "-")) orderByFields[orderByField.substring(1)] = -1;
        else if (_s.startsWith(orderByField, "+")) orderByFields[orderByField.substring(1)] = 1;
        else orderByFields[orderByField] = 1;
        return orderByFields;
    }, {});
}

function getTopOrLimit(query) {
    let limit = _defaultLimit;
    if (query.$top) limit = parseInt(query.$top);
    else if (query.$limit) limit = parseInt(query.$limit);

    if (isNaN(limit)) {
        limit = _defaultLimit;
    }

    return limit;
}

function getSkip(query) {
    if (!query.$skip) return 0;
    let skip = parseInt(query.$skip);
    if (isNaN(skip)) skip = 0;
    return skip;
}


function isMergeableObject(o){
    if (Array.isArray(o))return true;

    const isObject=(val)=> {
        return val != null && typeof val === 'object' && Array.isArray(val) === false;
    };

    const isObjectObject=(o)=> {
        return isObject(o) === true
            && Object.prototype.toString.call(o) === '[object Object]';
    };


    let ctor,prot;

    if (isObjectObject(o) === false) return false;

    // If has modified constructor
    ctor = o.constructor;
    if (typeof ctor !== 'function') return false;

    // If has modified prototype
    prot = ctor.prototype;
    if (isObjectObject(prot) === false) return false;

    // If constructor does not have an Object-specific method
    if (prot.hasOwnProperty('isPrototypeOf') === false) {
        return false;
    }

    // Most likely a plain Object
    return true;
}

const emptyTarget = value => Array.isArray(value) ? [] : {};
const clone = (value, options) => $merge(emptyTarget(value), value, options);
function combineMerge(target, source, options) {
    const destination = target.slice();

    source.forEach(function(e, i) {
        if (typeof destination[i] === 'undefined') {
            const cloneRequested = options.clone !== false
            const shouldClone = cloneRequested && options.isMergeableObject(e)
            destination[i] = shouldClone ? clone(e, options) : e
        } else if (options.isMergeableObject(e)) {
            destination[i] = $merge(target[i], e, options)
        } else if (target.indexOf(e) === -1) {
            destination.push(e)
        }
    });
    return destination
}


function getQuery(parsedQuery, defaults) {

    let where = {};
    if (parsedQuery.filter)
        where = $merge(where, parsedQuery.filter,{isMergeableObject:isMergeableObject,arrayMerge: combineMerge });
    if (parsedQuery.rawQuery)
        where = $merge(where, parsedQuery.rawQuery,{isMergeableObject:isMergeableObject,arrayMerge: combineMerge });
    if (defaults)
        where = $merge(where, defaults,{isMergeableObject:isMergeableObject,arrayMerge: combineMerge });

    return Object.keys(where).length === 0 ? null : where;


}

module.exports = MongoQuery;
