const ObjectID = require('mongodb').ObjectID;
const $check=require('check-types');

class Utils{
    static generateId () {
        return new ObjectID();
    }
    static isValidId (id) {
        if (!id) {
            return false;
        }

        if ($check.object(id)&&id._bsontype==="ObjectID"&&Buffer.isBuffer(id.id)&&id.id.length===12){
            return  ObjectID.isValid(id.id);
        }

        if (id.toString().length !== 24) {
            return false;
        }

        return ObjectID.isValid(id.toString());
    }


    static parseId (id) {

        if (!id) {
            return false;
        }

        if ($check.object(id)&&id._bsontype==="ObjectID"&&Buffer.isBuffer(id.id)&&id.id.length===12){
            return  new ObjectID(id.id);
        }

        if (id.toString().length !== 24) {
            return null;
        }

        if (ObjectID.isValid(id.toString())){
            return new ObjectID(id.toString());
        }
    }
}

module.exports = Utils;