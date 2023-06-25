const assert = require('assert');
const {ObjectId} = require('mongodb');
const cloneDeep = require('clone-deep');
const utils = require('../lib/Utils.js');

describe('Utils', function () {
    it('should validate id', function () {
        assert(utils.isValidId(new ObjectId()), 'Invalid valid');
        assert(utils.isValidId(new ObjectId().toString()), 'Invalid valid');
        assert(utils.isValidId(cloneDeep(new ObjectId())), 'Invalid valid');
        assert(!utils.isValidId(null), 'Invalid valid');
        assert(!utils.isValidId('xxx'), 'Invalid valid');
        assert(!utils.isValidId({a: 1}), 'Invalid valid');
    });

    it('should parse id', function () {
        const objId = new ObjectId();

        assert.strictEqual(utils.parseId(objId.toString()).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId(objId).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId(cloneDeep(objId)).toString(), objId.toString(), 'Invalid parse');
        assert.strictEqual(utils.parseId('xxx'), null, 'Invalid parse');
    });

    it('should get aggregate collections', function () {
        assert.deepStrictEqual(
            utils.getCollectionPathsForAggregate([
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes2',
                        as: 'cn2',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $project: {
                        c: '$c',
                        cn: '$cn',
                        cn2: '$cn2',
                    },
                },
            ]),
            [
                {
                    path: '/1/$lookup/from',
                    collection: 'customer-notes',
                },
                {
                    path: '/3/$lookup/from',
                    collection: 'customer-notes2',
                },
            ],
            'Invalid collections'
        );

        assert.deepStrictEqual(
            utils.getCollectionPathsForAggregate([
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $graphLookup: {
                        from: 'customer-notes2',
                        as: 'cn2',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $project: {
                        c: '$c',
                        cn: '$cn',
                        cn2: '$cn2',
                    },
                },
            ]),
            [
                {
                    path: '/1/$lookup/from',
                    collection: 'customer-notes',
                },
                {
                    path: '/3/$graphLookup/from',
                    collection: 'customer-notes2',
                },
            ],
            'Invalid collections'
        );

        assert.deepStrictEqual(
            utils.getCollectionPathsForAggregate([
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $unionWith: {
                        coll: 'customer-notes2',
                        as: 'cn2',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $project: {
                        c: '$c',
                        cn: '$cn',
                        cn2: '$cn2',
                    },
                },
            ]),
            [
                {
                    path: '/1/$lookup/from',
                    collection: 'customer-notes',
                },
                {
                    path: '/3/$unionWith/coll',
                    collection: 'customer-notes2',
                },
            ],
            'Invalid collections'
        );

        assert.deepStrictEqual(
            utils.getCollectionPathsForAggregate([
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $unionWith: 'customer-notes2',
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $project: {
                        c: '$c',
                        cn: '$cn',
                        cn2: '$cn2',
                    },
                },
            ]),
            [
                {
                    path: '/1/$lookup/from',
                    collection: 'customer-notes',
                },
                {
                    path: '/3/$unionWith',
                    collection: 'customer-notes2',
                },
            ],
            'Invalid collections'
        );

        assert.deepStrictEqual(
            utils.getCollectionPathsForAggregate([
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        pipeline: [
                            {
                                $project: {
                                    c: '$$ROOT',
                                },
                            },
                            {
                                $lookup: {
                                    from: 'customer-notes-pl',
                                    as: 'cn',
                                    localField: 'c.id',
                                    foreignField: 'id',
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $gt: [
                                            {
                                                $size: '$cn',
                                            },
                                            0,
                                        ],
                                    },
                                },
                            },
                            {
                                $unionWith: 'customer-notes2-pl',
                            },
                            {
                                $match: {
                                    $expr: {
                                        $gt: [
                                            {
                                                $size: '$cn2',
                                            },
                                            0,
                                        ],
                                    },
                                },
                            },
                            {
                                $project: {
                                    c: '$c',
                                    cn: '$cn',
                                    cn2: '$cn2',
                                },
                            },
                        ],
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $unionWith: 'customer-notes2',
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $project: {
                        c: '$c',
                        cn: '$cn',
                        cn2: '$cn2',
                    },
                },
            ]),
            [
                {
                    path: '/1/$lookup/from',
                    collection: 'customer-notes',
                },
                {
                    collection: 'customer-notes-pl',
                    path: '/1/$lookup/pipeline/1/$lookup/from',
                },
                {
                    collection: 'customer-notes2-pl',
                    path: '/1/$lookup/pipeline/3/$unionWith',
                },
                {
                    path: '/3/$unionWith',
                    collection: 'customer-notes2',
                },
            ],
            'Invalid collections'
        );
    });

    it('should clean aggregate', function () {
        assert.deepStrictEqual(
            utils.cleanAggregate(
                [
                    {
                        $project: {
                            c: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'customer-notes',
                            as: 'cn',
                            localField: 'c.id',
                            foreignField: 'id',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$cn',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $lookup: {
                            from: 'customer-notes2',
                            as: 'cn2',
                            localField: 'c.id',
                            foreignField: 'id',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$cn2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $out: 'test',
                    },
                ],
                false
            ),
            [
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes2',
                        as: 'cn2',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
            ],
            'Invalid clean'
        );

        assert.deepStrictEqual(
            utils.cleanAggregate(
                [
                    {
                        $project: {
                            c: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'customer-notes',
                            as: 'cn',
                            localField: 'c.id',
                            foreignField: 'id',
                            pipeline: [
                                {
                                    $project: {
                                        c: '$$ROOT',
                                    },
                                },
                                {$out: 'test'},
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$cn',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $lookup: {
                            from: 'customer-notes2',
                            as: 'cn2',
                            localField: 'c.id',
                            foreignField: 'id',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$cn2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $out: 'test',
                    },
                ],
                false
            ),
            [
                {
                    $project: {
                        c: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes',
                        as: 'cn',
                        localField: 'c.id',
                        foreignField: 'id',
                        pipeline: [
                            {
                                $project: {
                                    c: '$$ROOT',
                                },
                            },
                        ],
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn',
                                },
                                0,
                            ],
                        },
                    },
                },
                {
                    $lookup: {
                        from: 'customer-notes2',
                        as: 'cn2',
                        localField: 'c.id',
                        foreignField: 'id',
                    },
                },
                {
                    $match: {
                        $expr: {
                            $gt: [
                                {
                                    $size: '$cn2',
                                },
                                0,
                            ],
                        },
                    },
                },
            ],
            'Invalid clean'
        );

        assert.throws(
            () => {
                utils.cleanAggregate([
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$cn',
                                    },
                                    0,
                                ],
                            },
                        },
                        $where: 'let x;',
                    },
                    {
                        $out: 'test',
                    },
                ]);
            },
            Error,
            'Invalid clean'
        );
    });
    it('should not throw an error when cleaning an aggregate that has a function that is a substring of an unsafe function', function () {
        const safePipeline = [
            {
                $match: {
                    id: {
                        $eq: 1,
                    },
                },
            },
            {
                $replaceRoot: {
                    newRoot: {
                        $mergeObjects: [
                            '$$ROOT',
                            {
                                FullName: {
                                    $concat: [
                                        '$First Name',
                                        {
                                            $literal: '-',
                                        },
                                        '$Last Name',
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        ];
        assert.doesNotThrow(() => {
            utils.cleanAggregate(safePipeline);
        });
    });
    it('should throw an error if there is an unsafe function when calling clean aggregate', () => {
        const unsafePipeline = [
            {
                $merge: {
                    id: {
                        $eq: 1,
                    },
                },
            },
        ];
        assert.throws(() => {
            utils.cleanAggregate(unsafePipeline);
        });
    });
});
