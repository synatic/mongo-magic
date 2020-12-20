const assert = require('assert');
const {ObjectID} = require('mongodb');
const cloneDeep = require('clone-deep');
const utils = require('../lib/utils');

describe('Utils', function () {
    it('should validate id', function () {
        assert(utils.isValidId(new ObjectID()), 'Invalid valid');
        assert(utils.isValidId(new ObjectID().toString()), 'Invalid valid');
        assert(utils.isValidId(cloneDeep(new ObjectID())), 'Invalid valid');
        assert(!utils.isValidId(null), 'Invalid valid');
        assert(!utils.isValidId('xxx'), 'Invalid valid');
        assert(!utils.isValidId({a: 1}), 'Invalid valid');
    });

    it('should parse id', function () {
        const objId = new ObjectID();

        assert.strictEqual(utils.parseId(objId.toString()).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId(objId).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId(cloneDeep(objId)).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId('xxx'), null, 'Invalid parse');
    });
});
