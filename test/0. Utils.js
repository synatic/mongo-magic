const assert = require('assert');
const $mongodb=require('mongodb');
const $copy=require('clone-deep');
const utils=require('../lib/utils');

describe('Utils', function () {

    it('should validate id', function () {
        assert(utils.isValidId(new $mongodb.ObjectID()),"Invalid valid")
        assert(utils.isValidId((new $mongodb.ObjectID()).toString()),"Invalid valid")
        assert(utils.isValidId($copy((new $mongodb.ObjectID()))),"Invalid valid")
        assert(!utils.isValidId(null),"Invalid valid")
        assert(!utils.isValidId("xxx"),"Invalid valid")
        assert(!utils.isValidId({a:1}),"Invalid valid")
    });

    it('should parse id', function () {
        let objId=new $mongodb.ObjectID();


        assert.equal(utils.parseId(objId.toString()).toString(),objId.toString(),"Invalid parse")
        assert.equal(utils.parseId(objId).toString(),objId.toString(),"Invalid parse")
        assert.equal(utils.parseId($copy(objId)).toString(),objId.toString(),"Invalid parse")
        assert.equal(utils.parseId("xxx"),null,"Invalid parse")
    });

});
