/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

/** Options for the {@link GlideBf.reserve | BF.RESERVE} command. */
export interface BfReserveOptions {
    /** Expansion factor when filter is full. Default varies by implementation. */
    expansion?: number;
    /** If set, prevents auto-scaling when capacity is reached. */
    nonScaling?: boolean;
}

/** Options for the {@link GlideBf.insert | BF.INSERT} command. */
export interface BfInsertOptions {
    /** Initial capacity. */
    capacity?: number;
    /** Desired error rate (between 0 and 1). */
    errorRate?: number;
    /** Expansion factor. */
    expansion?: number;
    /** Prevents auto-scaling. */
    nonScaling?: boolean;
    /** If set, does not create a new filter if it does not already exist. */
    noCreate?: boolean;
}

/** Return type for the {@link GlideBf.info | BF.INFO} command. */
export interface BfInfoResult {
    capacity: number;
    size: number;
    numberOfFilters: number;
    numberOfItems: number;
    expansionRate: number;
}
