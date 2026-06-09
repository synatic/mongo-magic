const oDataParser = require('./odata/odata-parser.js');

/**
 * Defines a node in the parsing process
 */
class Node {
    /** Constructs a Filter node
     *
     * @param {string} type - the type of operator
     * @param {object} left - the left hand expression
     * @param {object} right - the right hand expression
     * @param {string} [func] - the function
     * @param {string|object|array} [args] - the args
     */
    constructor(type, left, right, func, args, name, value) {
        this.type = type;
        this.left = left;
        this.right = right;
        this.func = func;
        this.args = args;
        this.name = name;
        this.value = value;
    }

    /** Transforms the Node
     *
     * @return {{}}
     */
    transform() {
        const result = {};

        if (this.left?.type === 'property') {
            this.left.name = this.left.name.replace(/\//g, '.');
        }

        if (this.type === 'eq' && this.right?.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$eq: [left, this.right.value]}};
            }
            result[left] = this.right.value;
        }

        if (this.type === 'lt' && this.right?.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$lt: [left, this.right.value]}};
            }
            result[left] = {$lt: this.right.value};
        }

        if (this.type === 'gt' && this.right?.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$gt: [left, this.right.value]}};
            }
            result[left] = {$gt: this.right?.value};
        }

        if (this.type === 'ge' && this.right.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$gte: [left, this.right.value]}};
            }
            result[left] = {$gte: this.right.value};
        }

        if (this.type === 'le' && this.right.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$lte: [left, this.right.value]}};
            }
            result[left] = {$lte: this.right.value};
        }

        if (this.type === 'ne' && this.right.type === 'literal') {
            const left = new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform();
            if (typeof left === 'object') {
                return {$expr: {$ne: [left, this.right.value]}};
            }
            result[left] = {$ne: this.right.value};
        }

        if (this.type === 'and') {
            result['$and'] = result['$and'] || [];
            result['$and'].push(new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform());
            result['$and'].push(new Node(this.right.type, this.right.left, this.right.right, this.right.func, this.right.args, this.right.name, this.right.value).transform());
        }

        if (this.type === 'or') {
            result['$or'] = result['$or'] || [];
            result['$or'].push(new Node(this.left.type, this.left.left, this.left.right, this.left.func, this.left.args, this.left.name, this.left.value).transform());
            result['$or'].push(new Node(this.right.type, this.right.left, this.right.right, this.right.func, this.right.args, this.right.name, this.right.value).transform());
        }

        if (this.type === 'functioncall') {
            if (this.func === 'contains' || this.func === 'substringof') {
                const property = this.args[0].type === 'property' ? this.args[0].name : this.args[1].name;
                const literal = this.args[0].type === 'literal' ? this.args[0].value : this.args[1].value;
                result[property.replace(/\//g, '.')] = {$regex: literal, $options: 'i'};
            }

            if (this.func === 'startswith') {
                const property = this.args[0].type === 'property' ? this.args[0].name : this.args[1].name;
                const literal = this.args[0].type === 'literal' ? this.args[0].value : this.args[1].value;
                result[property.replace(/\//g, '.')] = {$regex: `^${literal}`, $options: 'i'};
            }

            if (this.func === 'endswith') {
                const property = this.args[0].type === 'property' ? this.args[0].name : this.args[1].name;
                const literal = this.args[0].type === 'literal' ? this.args[0].value : this.args[1].value;
                result[property.replace(/\//g, '.')] = {$regex: `${literal}$`, $options: 'i'};
            }

            if (this.func === 'tolower') {
                const property = new Node(this.args[0].type, this.args[0].left, this.args[0].right, this.args[0].func, this.args[0].args, this.args[0].name, this.args[0].value).transform();
                return {$toLower: `$${property}`};
            }

            if (this.func === 'toupper') {
                const property = new Node(this.args[0].type, this.args[0].left, this.args[0].right, this.args[0].func, this.args[0].args, this.args[0].name, this.args[0].value).transform();
                return {$toUpper: `$${property}`};
            }
        }

        if (this.type === 'property') {
            return this.name.replace(/\//g, '.');
        }

        if (this.type === 'literal') {
            return this.value;
        }

        return result;
    }
}

/**
 * Defines an oData to Mongo query parser
 */
class Parser {
    /** Parses an oData $filter expression
     *
     * @param {string} filterString - the oData filter string
     * @return {{}}
     */
    static parse(filterString) {
        const encodedQuery = decodeURIComponent('$filter=' + filterString);
        const encodedFilter = oDataParser.parse(encodedQuery);
        if (encodedFilter.error) {
            throw new Error(encodedFilter.error);
        }
        return new Node(
            encodedFilter.$filter.type,
            encodedFilter.$filter.left,
            encodedFilter.$filter.right,
            encodedFilter.$filter.func,
            encodedFilter.$filter.args,
            encodedFilter.$filter.name,
            encodedFilter.$filter.value
        ).transform();
    }
}

module.exports = Parser;
