/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

import {
    Decoder,
    DecoderOption,
    GlideClient,
    GlideClusterClient,
    GlideString,
} from "..";
import {
    BfReserveOptions,
    BfInsertOptions,
    BfInfoResult,
} from "./GlideBfOptions";

/** Module for Bloom Filter commands. */
export class GlideBf {
    /**
     * Creates an empty Bloom filter with a given desired error ratio and initial capacity.
     *
     * @param client - The client to execute the command.
     * @param key - The key under which the filter is created.
     * @param errorRate - The desired probability for false positives, between 0 and 1 (exclusive).
     *     For example, for a desired false positive rate of 0.1% (1 in 1000), the value should be 0.001.
     * @param capacity - The number of entries intended to be added to the filter.
     * @param options - (Optional) See {@link BfReserveOptions}.
     * @returns `"OK"` if the filter was created successfully.
     *
     * @example
     * ```typescript
     * await GlideBf.reserve(client, "myfilter", 0.001, 10000);
     * // "OK"
     * ```
     */
    static async reserve(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        errorRate: number,
        capacity: number,
        options?: BfReserveOptions,
    ): Promise<"OK"> {
        const args: GlideString[] = [
            "BF.RESERVE",
            key,
            errorRate.toString(),
            capacity.toString(),
        ];

        if (options?.expansion !== undefined) {
            args.push("EXPANSION", options.expansion.toString());
        }

        if (options?.nonScaling) {
            args.push("NONSCALING");
        }

        return _handleCustomCommand(client, args, {
            decoder: Decoder.String,
        }) as Promise<"OK">;
    }

    /**
     * Adds an item to a Bloom filter. Creates the filter if it does not yet exist.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param item - The item to add.
     * @returns `true` if the item was newly added, `false` if it may have existed previously.
     *
     * @example
     * ```typescript
     * const added = await GlideBf.add(client, "myfilter", "item1");
     * console.log(added); // true
     * ```
     */
    static async add(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        item: GlideString,
    ): Promise<boolean> {
        const args: GlideString[] = ["BF.ADD", key, item];
        const result = await _handleCustomCommand<number>(client, args);
        return result === 1;
    }

    /**
     * Adds one or more items to a Bloom filter. Creates the filter if it does not yet exist.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param items - One or more items to add.
     * @returns An array of booleans. `true` if the corresponding item was newly added,
     *     `false` if it may have existed previously.
     *
     * @example
     * ```typescript
     * const results = await GlideBf.madd(client, "myfilter", ["item1", "item2", "item3"]);
     * console.log(results); // [true, true, false]
     * ```
     */
    static async madd(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        items: GlideString[],
    ): Promise<boolean[]> {
        const args: GlideString[] = ["BF.MADD", key, ...items];
        const result = await _handleCustomCommand<number[]>(client, args);
        return result.map((v) => v === 1);
    }

    /**
     * Checks whether an item may exist in a Bloom filter.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param item - The item to check.
     * @returns `true` if the item may exist in the filter, `false` if it definitely does not.
     *
     * @example
     * ```typescript
     * const exists = await GlideBf.exists(client, "myfilter", "item1");
     * console.log(exists); // true
     * ```
     */
    static async exists(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        item: GlideString,
    ): Promise<boolean> {
        const args: GlideString[] = ["BF.EXISTS", key, item];
        const result = await _handleCustomCommand<number>(client, args);
        return result === 1;
    }

    /**
     * Checks whether one or more items may exist in a Bloom filter.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param items - One or more items to check.
     * @returns An array of booleans. `true` if the corresponding item may exist,
     *     `false` if it definitely does not.
     *
     * @example
     * ```typescript
     * const results = await GlideBf.mexists(client, "myfilter", ["item1", "item2", "missing"]);
     * console.log(results); // [true, true, false]
     * ```
     */
    static async mexists(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        items: GlideString[],
    ): Promise<boolean[]> {
        const args: GlideString[] = ["BF.MEXISTS", key, ...items];
        const result = await _handleCustomCommand<number[]>(client, args);
        return result.map((v) => v === 1);
    }

    /**
     * Returns information about a Bloom filter.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @returns A {@link BfInfoResult} object with filter metadata.
     *
     * @example
     * ```typescript
     * const info = await GlideBf.info(client, "myfilter");
     * console.log(info);
     * // { capacity: 10000, size: 7328, numberOfFilters: 1, numberOfItems: 3, expansionRate: 2 }
     * ```
     */
    static async info(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
    ): Promise<BfInfoResult> {
        const args: GlideString[] = ["BF.INFO", key];
        const result = await _handleCustomCommand<
            (GlideString | number)[]
        >(client, args);
        return _parseBfInfoResult(result);
    }

    /**
     * Adds one or more items to a Bloom filter. Creates the filter with specified parameters
     * if it does not yet exist.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param items - One or more items to add.
     * @param options - (Optional) See {@link BfInsertOptions}.
     * @returns An array of booleans. `true` if the corresponding item was newly added,
     *     `false` if it may have existed previously.
     *
     * @example
     * ```typescript
     * const results = await GlideBf.insert(client, "myfilter", ["item1", "item2"], {
     *     capacity: 10000,
     *     errorRate: 0.001,
     * });
     * console.log(results); // [true, true]
     * ```
     */
    static async insert(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        items: GlideString[],
        options?: BfInsertOptions,
    ): Promise<boolean[]> {
        const args: GlideString[] = ["BF.INSERT", key];

        if (options?.capacity !== undefined) {
            args.push("CAPACITY", options.capacity.toString());
        }

        if (options?.errorRate !== undefined) {
            args.push("ERROR", options.errorRate.toString());
        }

        if (options?.expansion !== undefined) {
            args.push("EXPANSION", options.expansion.toString());
        }

        if (options?.nonScaling) {
            args.push("NONSCALING");
        }

        if (options?.noCreate) {
            args.push("NOCREATE");
        }

        args.push("ITEMS", ...items);

        const result = await _handleCustomCommand<number[]>(client, args);
        return result.map((v) => v === 1);
    }

    /**
     * Returns the cardinality of a Bloom filter - the number of items that were added to the filter
     * and detected as unique (not already existing in the filter).
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @returns The number of items added to the filter. Returns 0 if the key does not exist.
     *
     * @example
     * ```typescript
     * const count = await GlideBf.card(client, "myfilter");
     * console.log(count); // 3
     * ```
     */
    static async card(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
    ): Promise<number> {
        const args: GlideString[] = ["BF.CARD", key];
        return _handleCustomCommand<number>(client, args);
    }

    /**
     * Restores a Bloom filter previously saved using BF.SCANDUMP.
     *
     * @param client - The client to execute the command.
     * @param key - The key of the Bloom filter.
     * @param data - The serialized Bloom filter data.
     * @returns `"OK"` if the filter was restored successfully.
     *
     * @example
     * ```typescript
     * await GlideBf.load(client, "myfilter", serializedData);
     * // "OK"
     * ```
     */
    static async load(
        client: GlideClient | GlideClusterClient,
        key: GlideString,
        data: GlideString,
    ): Promise<"OK"> {
        const args: GlideString[] = ["BF.LOADCHUNK", key, "0", data];
        return _handleCustomCommand(client, args, {
            decoder: Decoder.String,
        }) as Promise<"OK">;
    }
}

/**
 * @internal
 * Parses the flat array response from BF.INFO into a structured object.
 * BF.INFO returns alternating key-value pairs:
 * ["Capacity", 10000, "Size", 7328, "Number of filters", 1, "Number of items inserted", 3, "Expansion rate", 2]
 */
function _parseBfInfoResult(
    result: (GlideString | number)[],
): BfInfoResult {
    const info: BfInfoResult = {
        capacity: 0,
        size: 0,
        numberOfFilters: 0,
        numberOfItems: 0,
        expansionRate: 0,
    };

    for (let i = 0; i < result.length; i += 2) {
        const key = String(result[i]).toLowerCase();
        const value = result[i + 1] as number;

        if (key === "capacity") {
            info.capacity = value;
        } else if (key === "size") {
            info.size = value;
        } else if (key.includes("number of filters")) {
            info.numberOfFilters = value;
        } else if (key.includes("number of items")) {
            info.numberOfItems = value;
        } else if (key.includes("expansion")) {
            info.expansionRate = value;
        }
    }

    return info;
}

/**
 * @internal
 */
async function _handleCustomCommand<T>(
    client: GlideClient | GlideClusterClient,
    args: GlideString[],
    decoderOption: DecoderOption = {},
): Promise<T> {
    return client instanceof GlideClient
        ? ((client as GlideClient).customCommand(
              args,
              decoderOption,
          ) as Promise<T>)
        : ((client as GlideClusterClient).customCommand(
              args,
              decoderOption,
          ) as Promise<T>);
}
