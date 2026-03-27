/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { GlideFt, GlideRecord, GlideString, SortOrder } from "..";

interface BaseField {
    /** The name of the field. */
    name: GlideString;
    /** An alias for field. */
    alias?: GlideString;
    /** If set, the field is sortable. Allows using the field in `SORTBY` clauses of {@link GlideFt.search | FT.SEARCH}. */
    sortable?: boolean;
}

/**
 * Field contains any blob of data.
 */
export type TextField = BaseField & {
    /** Field identifier */
    type: "TEXT";
    /** If set, disables stemming when indexing this field. */
    nostem?: boolean;
    /** The weight of this field in scoring. Default is `1.0`. */
    weight?: number;
    /** If set, keeps a suffix trie with all terms which match the suffix. Used to optimize `*foo*` queries. */
    withsuffixtrie?: boolean;
    /** If set, removes an existing suffix trie from the field. */
    nosuffixtrie?: boolean;
};

/**
 * Tag fields are similar to full-text fields, but they interpret the text as a simple list of
 * tags delimited by a separator character.
 *
 * For HASH fields, separator default is a comma (`,`). For JSON fields, there is no default
 * separator; you must declare one explicitly if needed.
 */
export type TagField = BaseField & {
    /** Field identifier */
    type: "TAG";
    /** Specify how text in the attribute is split into individual tags. Must be a single character. */
    separator?: GlideString;
    /** Preserve the original letter cases of tags. If set to `false`, characters are converted to lowercase by default. */
    caseSensitive?: boolean;
};

/**
 * Field contains a number.
 */
export type NumericField = BaseField & {
    /** Field identifier */
    type: "NUMERIC";
};

/**
 * Superclass for vector field implementations, contains common logic.
 */
export type VectorField = BaseField & {
    /** Field identifier */
    type: "VECTOR";
    /** Additional attributes to be passed with the vector field after the algorithm name. */
    attributes: VectorFieldAttributesFlat | VectorFieldAttributesHnsw;
};

/**
 * Base class for defining vector field attributes to be used after the vector algorithm name.
 */
interface VectorFieldAttributes {
    /** Number of dimensions in the vector. Equivalent to `DIM` in the module API. */
    dimensions: number;
    /**
     * The distance metric used in vector type field. Can be one of `[L2 | IP | COSINE]`. Equivalent to `DISTANCE_METRIC` in the module API.
     */
    distanceMetric: "L2" | "IP" | "COSINE";
    /** Vector type. The only supported type is FLOAT32. */
    type?: "FLOAT32";
    /**
     * Initial vector capacity in the index affecting memory allocation size of the index. Defaults to `1024`. Equivalent to `INITIAL_CAP` in the module API.
     */
    initialCap?: number;
}

/**
 * Vector field that supports vector search by FLAT (brute force) algorithm.
 *
 * The algorithm is a brute force linear processing of each vector in the index, yielding exact
 * answers within the bounds of the precision of the distance computations.
 */
export type VectorFieldAttributesFlat = VectorFieldAttributes & {
    algorithm: "FLAT";
};

/**
 * Vector field that supports vector search by HNSM (Hierarchical Navigable Small World) algorithm.
 *
 * The algorithm provides an approximation of the correct answer in exchange for substantially
 * lower execution times.
 */
export type VectorFieldAttributesHnsw = VectorFieldAttributes & {
    algorithm: "HNSW";
    /**
     * Number of maximum allowed outgoing edges for each node in the graph in each layer. Default is `16`, maximum is `512`.
     * Equivalent to `M` in the module API.
     */
    numberOfEdges?: number;
    /**
     * Controls the number of vectors examined during index construction. Default value is `200`, Maximum value is `4096`.
     * Equivalent to `EF_CONSTRUCTION` in the module API.
     */
    vectorsExaminedOnConstruction?: number;
    /**
     * Controls the number of vectors examined during query operations. Default value is `10`, Maximum value is `4096`.
     * Equivalent to `EF_RUNTIME` in the module API.
     */
    vectorsExaminedOnRuntime?: number;
};

export type Field = TextField | TagField | NumericField | VectorField;

/**
 * Represents the input options to be used in the {@link GlideFt.create | FT.CREATE} command.
 * All fields in this class are optional inputs for FT.CREATE.
 */
export interface FtCreateOptions {
    /** The type of data to be indexed using FT.CREATE. */
    dataType: "JSON" | "HASH";
    /** The prefix of the key to be indexed. */
    prefixes?: GlideString[];
    /** Default score for documents in the index. Must be between 0.0 and 1.0. Default is `1.0`. */
    score?: number;
    /** Default language for documents in the index (e.g. `"english"`, `"spanish"`). Used for stemming during indexing and search. */
    language?: string;
    /** If set, skips the initial scan of existing keys when creating the index. */
    skipInitialScan?: boolean;
    /** Minimum stem length for stemming. */
    minStemSize?: number;
    /** If set, keeps term offsets in the index. Required for exact phrase matching. */
    withOffsets?: boolean;
    /** If set, does not store term offsets in the index. Saves memory but disables exact phrase matching. */
    noOffsets?: boolean;
    /** If set, does not use stop words for this index. */
    noStopWords?: boolean;
    /** A list of custom stop words. If provided, the default stop words are replaced by these. Use an empty array to disable stop words. */
    stopWords?: GlideString[];
    /** Custom punctuation characters for tokenization. */
    punctuation?: GlideString;
}

/**
 * Represents the input options to be used in the FT.SEARCH command.
 * All fields in this class are optional inputs for FT.SEARCH.
 */
export type FtSearchOptions = {
    /** Query timeout in milliseconds. */
    timeout?: number;

    /**
     * Add a field to be returned.
     * @param fieldIdentifier field name to return.
     * @param alias optional alias for the field name to return.
     */
    returnFields?: { fieldIdentifier: GlideString; alias?: GlideString }[];

    /**
     * Query parameters, which could be referenced in the query by `$` sign, followed by
     * the parameter name.
     */
    params?: GlideRecord<GlideString>;

    /** If set, returns only the number of matching documents and their IDs, without the document content. */
    nocontent?: boolean;
    /** The query dialect version to use. See Valkey Search documentation for supported dialect versions. */
    dialect?: number;
    /** If set, the query terms are used as-is without stemming. */
    verbatim?: boolean;
    /** If set, requires all query terms to appear in the same order in the document. Usually used together with `slop`. */
    inorder?: boolean;
    /** The maximum number of intervening terms allowed between query terms for them to be considered a match. Used with `inorder`. */
    slop?: number;
    /** Sort the results by the given field. */
    sortby?: { field: GlideString; order?: SortOrder };
    /** Use a custom scoring function. See Valkey Search documentation for available scorers. */
    scorer?: GlideString;
} & (
    | {
          /**
           * Configure query pagination. By default only first 10 documents are returned.
           *
           * @param offset Zero-based offset.
           * @param count Number of elements to return.
           */
          limit?: { offset: number; count: number };
          /** `limit` and `count` are mutually exclusive. */
          count?: never;
      }
    | {
          /**
           * Once set, the query will return only the number of documents in the result set without actually
           * returning them.
           */
          count?: boolean;
          /** `limit` and `count` are mutually exclusive. */
          limit?: never;
      }
);
