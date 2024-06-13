import {Filter, Document} from 'mongodb';
import {MomentInput} from 'moment';

export interface Query<TSchema extends Document> {
    /** A comma separated list of fields to select. Start with a `-` character to exclude it. Can only have all negatives or all positives in one query */
    $select?: string;
    /** A JSON string or a projection object */
    $projection?: string | ProjectionOrSort<TSchema>;
    /**
     * Comma separated string specifying the sort order for results.
     * Start with `+` or `-` for ascending/descending
     * Alternatively end with ` desc` or ` asc` for ascending/descending
     * Default ordering if neither are specified is ascending
     * @alias sort
     * @default ascending
     * @example
     * const query = {
     *      $orderBy: "+name,-description,price desc,date asc,index"
     * }
     * */
    $orderBy?: string;
    /**
     * $orderBy takes priority over $sort, see it for docs
     * @alias $orderBy
     */
    $sort?: string;
    /**
     * specifies how many results to return
     * @alias $limit
     * @default 50
     */
    $top?: number | string;
    /**
     * $top takes priority over limit
     * @alias $top
     */
    $limit?: number | string;
    /** Specifies how many results to skip */
    $skip?: number | string;
    /** A raw mongodb query or JSON string of one, will look through the object for $date, $objectId, $int, $float, $bool, $string */
    $rawQuery?: string | Filter<TSchema>;
    $filter?: string;
}

export interface ParsedQuery<TSchema extends Document> {
    select: Select<TSchema> | null;
    projection: ProjectionOrSort<TSchema> | null;
    orderBy: ProjectionOrSort<TSchema> | null;
    limit: number;
    top: number;
    sort: ProjectionOrSort<TSchema> | null;
    skip: number;
    rawQuery?: Filter<TSchema> | null;
    filter?: Filter<TSchema> | null;
    query: Filter<TSchema>;
}

export type Select<TSchema extends Document> = {
    [key in keyof TSchema]?: boolean;
};

export type ProjectionOrSort<TSchema extends Document> = {
    [key in keyof TSchema]?: 1 | -1;
};

export interface UpdateStatsOptions<TSchema extends Document> {
    statsField: string;
    increments: Increment[] | Increment;
    date: MomentInput;
    query: Filter<TSchema>;
}

export interface Increment {
    value: number;
    field: string;
}
