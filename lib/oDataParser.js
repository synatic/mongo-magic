const oDataParser = require('odata-parser');

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
    constructor(type, left, right, func, args) {
        this.type = type;
        this.left = left;
        this.right = right;
        this.func = func;
        this.args = args;
    }

    /** Transforms the Node
     *
     * @return {{}}
     */
    transform() {
        const result = {};

        if (this.left.name) {
            this.left.name = this.left.name.replace(/\//g, '.');
        }

        if (this.type === 'eq' && this.right.type === 'literal') {
            result[this.left.name] = this.right.value;
        }

        if (this.type === 'lt' && this.right.type === 'literal') {
            result[this.left.name] = {$lt: this.right.value};
        }

        if (this.type === 'gt' && this.right.type === 'literal') {
            result[this.left.name] = {$gt: this.right.value};
        }

        if (this.type === 'ge' && this.right.type === 'literal') {
            result[this.left.name] = {$gte: this.right.value};
        }

        if (this.type === 'le' && this.right.type === 'literal') {
            result[this.left.name] = {$lte: this.right.value};
        }

        if (this.type === 'ne' && this.right.type === 'literal') {
            result[this.left.name] = {$ne: this.right.value};
        }

        if (this.type === 'and') {
            result['$and'] = result['$and'] || [];
            result['$and'].push(new Node(this.left.type, this.left.left, this.left.right, this.func, this.args).transform());
            result['$and'].push(new Node(this.right.type, this.right.left, this.right.right, this.func, this.args).transform());
        }

        if (this.type === 'or') {
            result['$or'] = result['$or'] || [];
            result['$or'].push(new Node(this.left.type, this.left.left, this.left.right, this.func, this.args).transform());
            result['$or'].push(new Node(this.right.type, this.right.left, this.right.right, this.func, this.args).transform());
        }

        if (this.type === 'functioncall') {
            switch (this.func) {
                case 'substringof':
                    substringof(this, result);
            }
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
            encodedFilter.$filter.args
        ).transform();
    }
}

module.exports = Parser;

/**
 * This function was not defined. Find out from Martin what's supposed to go here
 */
function substringof() {
    // todo: finish implementation
    console.log('mongo-magic: substringof not implemented');
}
