/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */
import {
    afterAll,
    afterEach,
    beforeAll,
    describe,
    expect,
    it,
} from "@jest/globals";
import { ValkeyCluster } from "../../utils/TestUtils";
import {
    ClusterBatch,
    ConditionalChange,
    Decoder,
    FtSearchOptions,
    FtSearchReturnType,
    GlideClusterClient,
    GlideBf,
    GlideFt,
    GlideJson,
    GlideString,
    InfoOptions,
    JsonGetOptions,
    ProtocolVersion,
    RequestError,
    SortOrder,
    VectorField,
} from "../build-ts";
import {
    CreateJsonBatchCommands,
    flushAndCloseClient,
    getClientConfigurationOption,
    getRandomKey,
    getServerVersion,
    JsonBatchForArrCommands,
    parseEndpoints,
    validateBatchResponse,
} from "./TestUtilities";

const TIMEOUT = 50000;
/** Waiting interval to let server process the data before querying */
const DATA_PROCESSING_TIMEOUT = 1000;

describe("Server Module Tests", () => {
    let cluster: ValkeyCluster;

    beforeAll(async () => {
        const clusterAddresses = global.CLUSTER_ENDPOINTS;
        cluster = await ValkeyCluster.initFromExistingCluster(
            true,
            parseEndpoints(clusterAddresses),
            getServerVersion,
        );
    }, 40000);

    afterAll(async () => {
        await cluster.close();
    }, TIMEOUT);

    describe.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
        "GlideJson",
        (protocol) => {
            let client: GlideClusterClient;

            afterEach(async () => {
                await flushAndCloseClient(
                    true,
                    cluster?.getAddresses(),
                    client,
                );
            });

            it("check modules loaded", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const info = await client.info({
                    sections: [InfoOptions.Modules],
                    route: "randomNode",
                });
                expect(info).toContain("# json_core_metrics");
                expect(info).toContain("# search_index_stats");
            });

            it("json.set and json.get tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = { a: 1.0, b: 2 };

                // JSON.set
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                // JSON.get
                let result = await GlideJson.get(client, key, { path: "." });
                expect(JSON.parse((result as string).toString())).toEqual(
                    jsonValue,
                );

                // binary buffer test
                result = await GlideJson.get(client, Buffer.from(key), {
                    path: Buffer.from("."),
                    decoder: Decoder.Bytes,
                });
                expect(result).toEqual(Buffer.from(JSON.stringify(jsonValue)));

                expect(
                    await GlideJson.set(
                        client,
                        Buffer.from(key),
                        Buffer.from("$"),
                        Buffer.from(JSON.stringify({ a: 1.0, b: 3 })),
                    ),
                ).toBe("OK");

                // JSON.get with array of paths
                result = await GlideJson.get(client, key, {
                    path: ["$.a", "$.b"],
                });
                expect(JSON.parse((result as string).toString())).toEqual({
                    "$.a": [1.0],
                    "$.b": [3],
                });

                // JSON.get with non-existing key
                expect(
                    await GlideJson.get(client, "non_existing_key", {
                        path: ["$"],
                    }),
                );

                // JSON.get with non-existing path
                result = await GlideJson.get(client, key, { path: "$.d" });
                expect(result).toEqual("[]");
            });

            it("json.set and json.get tests with multiple value", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();

                // JSON.set with complex object
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify({
                            a: { c: 1, d: 4 },
                            b: { c: 2 },
                            c: true,
                        }),
                    ),
                ).toBe("OK");

                // JSON.get with deep path
                let result = await GlideJson.get(client, key, {
                    path: "$..c",
                });
                expect(JSON.parse((result as string).toString())).toEqual([
                    true,
                    1,
                    2,
                ]);

                // JSON.set with deep path
                expect(
                    await GlideJson.set(client, key, "$..c", '"new_value"'),
                ).toBe("OK");

                // verify JSON.set result
                result = await GlideJson.get(client, key, { path: "$..c" });
                expect(JSON.parse((result as string).toString())).toEqual([
                    "new_value",
                    "new_value",
                    "new_value",
                ]);
            });

            it("json.set conditional set", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const value = JSON.stringify({ a: 1.0, b: 2 });

                expect(
                    await GlideJson.set(client, key, "$", value, {
                        conditionalChange: ConditionalChange.ONLY_IF_EXISTS,
                    }),
                ).toBeNull();

                expect(
                    await GlideJson.set(client, key, "$", value, {
                        conditionalChange:
                            ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
                    }),
                ).toBe("OK");

                expect(
                    await GlideJson.set(client, key, "$.a", "4.5", {
                        conditionalChange:
                            ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
                    }),
                ).toBeNull();
                let result = await GlideJson.get(client, key, {
                    path: ".a",
                });
                expect(result).toEqual("1");

                expect(
                    await GlideJson.set(client, key, "$.a", "4.5", {
                        conditionalChange: ConditionalChange.ONLY_IF_EXISTS,
                    }),
                ).toBe("OK");
                result = await GlideJson.get(client, key, { path: ".a" });
                expect(result).toEqual("4.5");
            });

            it("json.get formatting", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                // Set initial JSON value
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify({ a: 1.0, b: 2, c: { d: 3, e: 4 } }),
                    ),
                ).toBe("OK");
                // JSON.get with formatting options
                let result = await GlideJson.get(client, key, {
                    path: "$",
                    indent: "  ",
                    newline: "\n",
                    space: " ",
                } as JsonGetOptions);

                const expectedResult1 =
                    '[\n  {\n    "a": 1,\n    "b": 2,\n    "c": {\n      "d": 3,\n      "e": 4\n    }\n  }\n]';
                expect(result).toEqual(expectedResult1);
                // JSON.get with different formatting options
                result = await GlideJson.get(client, key, {
                    path: "$",
                    indent: "~",
                    newline: "\n",
                    space: "*",
                } as JsonGetOptions);

                const expectedResult2 =
                    '[\n~{\n~~"a":*1,\n~~"b":*2,\n~~"c":*{\n~~~"d":*3,\n~~~"e":*4\n~~}\n~}\n]';
                expect(result).toEqual(expectedResult2);

                // binary buffer test
                const result3 = await GlideJson.get(client, Buffer.from(key), {
                    path: Buffer.from("$"),
                    indent: Buffer.from("~"),
                    newline: Buffer.from("\n"),
                    space: Buffer.from("*"),
                } as JsonGetOptions);
                expect(result3).toEqual(expectedResult2);
            });

            it("json.mget", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key1 = getRandomKey();
                const key2 = getRandomKey();
                const data = {
                    [key1]: '{"a": 1, "b": ["one", "two"]}',
                    [key2]: '{"a": 1, "c": false}',
                };

                for (const key of Object.keys(data)) {
                    await GlideJson.set(client, key, ".", data[key]);
                }

                expect(
                    await GlideJson.mget(
                        client,
                        [key1, key2, getRandomKey()],
                        Buffer.from("$.c"),
                    ),
                ).toEqual(["[]", "[false]", null]);
                expect(
                    await GlideJson.mget(
                        client,
                        [Buffer.from(key1), key2],
                        ".b[*]",
                        { decoder: Decoder.Bytes },
                    ),
                ).toEqual([Buffer.from('"one"'), null]);
            });

            it("json.arrinsert", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();
                const doc = {
                    a: [],
                    b: { a: [1, 2, 3, 4] },
                    c: { a: "not an array" },
                    d: [{ a: ["x", "y"] }, { a: [["foo"]] }],
                    e: [{ a: 42 }, { a: {} }],
                    f: { a: [true, false, null] },
                };
                expect(
                    await GlideJson.set(client, key, "$", JSON.stringify(doc)),
                ).toBe("OK");

                const result = await GlideJson.arrinsert(
                    client,
                    key,
                    "$..a",
                    0,
                    [
                        '"string_value"',
                        "123",
                        '{"key": "value"}',
                        "true",
                        "null",
                        '["bar"]',
                    ],
                );
                expect(result).toEqual([6, 10, null, 8, 7, null, null, 9]);

                const expected = {
                    a: [
                        "string_value",
                        123,
                        { key: "value" },
                        true,
                        null,
                        ["bar"],
                    ],
                    b: {
                        a: [
                            "string_value",
                            123,
                            { key: "value" },
                            true,
                            null,
                            ["bar"],
                            1,
                            2,
                            3,
                            4,
                        ],
                    },
                    c: { a: "not an array" },
                    d: [
                        {
                            a: [
                                "string_value",
                                123,
                                { key: "value" },
                                true,
                                null,
                                ["bar"],
                                "x",
                                "y",
                            ],
                        },
                        {
                            a: [
                                "string_value",
                                123,
                                { key: "value" },
                                true,
                                null,
                                ["bar"],
                                ["foo"],
                            ],
                        },
                    ],
                    e: [{ a: 42 }, { a: {} }],
                    f: {
                        a: [
                            "string_value",
                            123,
                            { key: "value" },
                            true,
                            null,
                            ["bar"],
                            true,
                            false,
                            null,
                        ],
                    },
                };
                expect(
                    JSON.parse((await GlideJson.get(client, key)) as string),
                ).toEqual(expected);

                // Binary buffer test
                expect(
                    JSON.parse(
                        (await GlideJson.get(
                            client,
                            Buffer.from(key),
                        )) as string,
                    ),
                ).toEqual(expected);
            });

            it("json.arrpop", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();
                let doc =
                    '{"a": [1, 2, true], "b": {"a": [3, 4, ["value", 3, false], 5], "c": {"a": 42}}}';
                expect(await GlideJson.set(client, key, "$", doc)).toBe("OK");

                let res = await GlideJson.arrpop(client, key, {
                    path: "$.a",
                    index: 1,
                });
                expect(res).toEqual(["2"]);

                res = await GlideJson.arrpop(client, Buffer.from(key), {
                    path: "$..a",
                });
                expect(res).toEqual(["true", "5", null]);

                res = await GlideJson.arrpop(client, key, {
                    path: "..a",
                    decoder: Decoder.Bytes,
                });
                expect(res).toEqual(Buffer.from("1"));

                // Even if only one array element was returned, ensure second array at `..a` was popped
                doc = (await GlideJson.get(client, key, {
                    path: ["$..a"],
                })) as string;
                expect(doc).toEqual("[[],[3,4],42]");

                // Out of index
                res = await GlideJson.arrpop(client, key, {
                    path: Buffer.from("$..a"),
                    index: 10,
                });
                expect(res).toEqual([null, "4", null]);

                // pop without options
                expect(await GlideJson.set(client, key, "$", doc)).toEqual(
                    "OK",
                );
                expect(await GlideJson.arrpop(client, key)).toEqual("42");

                // Binary buffer test
                expect(
                    await GlideJson.arrpop(client, Buffer.from(key)),
                ).toEqual("[3,4]");
            });

            it("json.arrlen", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();
                const doc =
                    '{"a": [1, 2, 3], "b": {"a": [1, 2], "c": {"a": 42}}}';
                expect(await GlideJson.set(client, key, "$", doc)).toBe("OK");

                expect(
                    await GlideJson.arrlen(client, key, { path: "$.a" }),
                ).toEqual([3]);
                expect(
                    await GlideJson.arrlen(client, key, { path: "$..a" }),
                ).toEqual([3, 2, null]);
                // Legacy path retrieves the first array match at ..a
                expect(
                    await GlideJson.arrlen(client, key, { path: "..a" }),
                ).toEqual(3);
                // Value at path is not an array
                expect(
                    await GlideJson.arrlen(client, key, { path: "$" }),
                ).toEqual([null]);

                await expect(
                    GlideJson.arrlen(client, key, { path: "." }),
                ).rejects.toThrow();

                expect(
                    await GlideJson.set(client, key, "$", "[1, 2, 3, 4]"),
                ).toBe("OK");
                expect(await GlideJson.arrlen(client, key)).toEqual(4);

                // Binary buffer test
                expect(
                    await GlideJson.arrlen(client, Buffer.from(key)),
                ).toEqual(4);
            });

            it("json.arrindex", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key1 = getRandomKey();
                const key2 = getRandomKey();
                const doc1 =
                    '{"a": [1, 3, true, "hello"], "b": {"a": [3, 4, [3, false], 5], "c": {"a": 42}}}';

                expect(await GlideJson.set(client, key1, "$", doc1)).toBe("OK");

                // Verify scalar type
                expect(
                    await GlideJson.arrindex(client, key1, "$..a", true),
                ).toEqual([2, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key1, "..a", true),
                ).toEqual(2);

                expect(
                    await GlideJson.arrindex(client, key1, "$..a", 3),
                ).toEqual([1, 0, null]);
                expect(
                    await GlideJson.arrindex(client, key1, "..a", 3),
                ).toEqual(1);

                expect(
                    await GlideJson.arrindex(client, key1, "$..a", '"hello"'),
                ).toEqual([3, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key1, "..a", '"hello"'),
                ).toEqual(3);

                expect(
                    await GlideJson.arrindex(client, key1, "$..a", null),
                ).toEqual([-1, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key1, "..a", null),
                ).toEqual(-1);

                // Value at the path is not an array
                expect(
                    await GlideJson.arrindex(client, key1, "$..c", 42),
                ).toEqual([null]);
                await expect(
                    GlideJson.arrindex(client, key1, "..c", 42),
                ).rejects.toThrow(RequestError);

                const doc2 =
                    '{"a": [1, 3, true, "foo", "meow", "m", "foo", "lol", false],' +
                    ' "b": {"a": [3, 4, ["value", 3, false], 5], "c": {"a": 42}}}';

                expect(await GlideJson.set(client, key2, "$", doc2)).toBe("OK");

                // Verify optional `start` and `end`
                expect(
                    await GlideJson.arrindex(client, key2, "$..a", '"foo"', {
                        start: 6,
                        end: 8,
                    }),
                ).toEqual([6, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key2, "$..a", '"foo"', {
                        start: 2,
                        end: 8,
                    }),
                ).toEqual([3, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key2, "..a", '"meow"', {
                        start: 2,
                        end: 8,
                    }),
                ).toEqual(4);

                // Verify without optional `end`
                expect(
                    await GlideJson.arrindex(client, key2, "$..a", '"foo"', {
                        start: 6,
                    }),
                ).toEqual([6, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key2, "..a", '"foo"', {
                        start: 6,
                    }),
                ).toEqual(6);

                // Verify optional `end` with 0 or -1 (means the last element is included)
                expect(
                    await GlideJson.arrindex(client, key2, "$..a", '"foo"', {
                        start: 6,
                        end: 0,
                    }),
                ).toEqual([6, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key2, "..a", '"foo"', {
                        start: 6,
                        end: 0,
                    }),
                ).toEqual(6);
                expect(
                    await GlideJson.arrindex(client, key2, "$..a", '"foo"', {
                        start: 6,
                        end: -1,
                    }),
                ).toEqual([6, -1, null]);
                expect(
                    await GlideJson.arrindex(client, key2, "..a", '"foo"', {
                        start: 6,
                        end: -1,
                    }),
                ).toEqual(6);

                // Test with binary input
                expect(
                    await GlideJson.arrindex(
                        client,
                        Buffer.from(key2),
                        Buffer.from("$..a"),
                        Buffer.from('"foo"'),
                        {
                            start: 6,
                            end: -1,
                        },
                    ),
                ).toEqual([6, -1, null]);
                expect(
                    await GlideJson.arrindex(
                        client,
                        Buffer.from(key2),
                        Buffer.from("..a"),
                        Buffer.from('"foo"'),
                        {
                            start: 6,
                            end: -1,
                        },
                    ),
                ).toEqual(6);

                // Test with non-existent path
                expect(
                    await GlideJson.arrindex(
                        client,
                        key2,
                        "$.nonexistent",
                        true,
                    ),
                ).toEqual([]);
                await expect(
                    GlideJson.arrindex(client, key2, "nonexistent", true),
                ).rejects.toThrow(RequestError);

                // Test with non-existent key
                await expect(
                    GlideJson.arrindex(client, "non_existing_key", "$", true),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.arrindex(client, "non_existing_key", ".", true),
                ).rejects.toThrow(RequestError);
            });

            it("json.toggle tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const key2 = getRandomKey();
                const jsonValue = {
                    bool: true,
                    nested: { bool: false, nested: { bool: 10 } },
                };
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.toggle(client, key, { path: "$..bool" }),
                ).toEqual([false, true, null]);
                expect(
                    await GlideJson.toggle(client, key, { path: "bool" }),
                ).toBe(true);
                expect(
                    await GlideJson.toggle(client, key, {
                        path: "$.non_existing",
                    }),
                ).toEqual([]);
                expect(
                    await GlideJson.toggle(client, key, { path: "$.nested" }),
                ).toEqual([null]);

                // testing behavior with default pathing
                expect(await GlideJson.set(client, key2, ".", "true")).toBe(
                    "OK",
                );
                expect(await GlideJson.toggle(client, key2)).toBe(false);
                expect(await GlideJson.toggle(client, key2)).toBe(true);

                // expect request errors
                await expect(
                    GlideJson.toggle(client, key, { path: "nested" }),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.toggle(client, key, { path: ".non_existing" }),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.toggle(client, "non_existing_key", { path: "$" }),
                ).rejects.toThrow(RequestError);

                // Binary buffer test
                expect(await GlideJson.toggle(client, Buffer.from(key2))).toBe(
                    false,
                );
            });

            it("json.del tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = { a: 1.0, b: { a: 1, b: 2.5, c: true } };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                // non-existing paths
                expect(
                    await GlideJson.del(client, key, { path: "$..path" }),
                ).toBe(0);
                expect(
                    await GlideJson.del(client, key, { path: "..path" }),
                ).toBe(0);

                // deleting existing path
                expect(await GlideJson.del(client, key, { path: "$..a" })).toBe(
                    2,
                );
                expect(await GlideJson.get(client, key, { path: "$..a" })).toBe(
                    "[]",
                );
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(await GlideJson.del(client, key, { path: "..a" })).toBe(
                    2,
                );
                await expect(
                    GlideJson.get(client, key, { path: "..a" }),
                ).rejects.toThrow(RequestError);

                // verify result
                const result = await GlideJson.get(client, key, {
                    path: "$",
                });
                expect(JSON.parse(result as string)).toEqual([
                    { b: { b: 2.5, c: true } },
                ]);

                // test root deletion operations
                expect(await GlideJson.del(client, key, { path: "$" })).toBe(1);

                // reset and test dot deletion
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(await GlideJson.del(client, key, { path: "." })).toBe(1);

                // reset and test key deletion
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(await GlideJson.del(client, key)).toBe(1);
                expect(await GlideJson.del(client, key)).toBe(0);
                expect(
                    await GlideJson.get(client, key, { path: "$" }),
                ).toBeNull();

                // Binary buffer test
                expect(await GlideJson.del(client, Buffer.from(key))).toBe(0);

                // non-existing keys
                expect(
                    await GlideJson.del(client, "non_existing_key", {
                        path: "$",
                    }),
                ).toBe(0);
                expect(
                    await GlideJson.del(client, "non_existing_key", {
                        path: ".",
                    }),
                ).toBe(0);
            });

            it("json.forget tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = { a: 1.0, b: { a: 1, b: 2.5, c: true } };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                // non-existing paths
                expect(
                    await GlideJson.forget(client, key, { path: "$..path" }),
                ).toBe(0);
                expect(
                    await GlideJson.forget(client, key, { path: "..path" }),
                ).toBe(0);

                // deleting existing paths
                expect(
                    await GlideJson.forget(client, key, { path: "$..a" }),
                ).toBe(2);
                expect(await GlideJson.get(client, key, { path: "$..a" })).toBe(
                    "[]",
                );
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.forget(client, key, { path: "..a" }),
                ).toBe(2);
                await expect(
                    GlideJson.get(client, key, { path: "..a" }),
                ).rejects.toThrow(RequestError);

                // verify result
                const result = await GlideJson.get(client, key, {
                    path: "$",
                });
                expect(JSON.parse(result as string)).toEqual([
                    { b: { b: 2.5, c: true } },
                ]);

                // test root deletion operations
                expect(await GlideJson.forget(client, key, { path: "$" })).toBe(
                    1,
                );

                // reset and test dot deletion
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(await GlideJson.forget(client, key, { path: "." })).toBe(
                    1,
                );

                // reset and test key deletion
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(await GlideJson.forget(client, key)).toBe(1);
                expect(await GlideJson.forget(client, key)).toBe(0);
                expect(
                    await GlideJson.get(client, key, { path: "$" }),
                ).toBeNull();

                // Binary buffer test
                expect(await GlideJson.forget(client, Buffer.from(key))).toBe(
                    0,
                );

                // non-existing keys
                expect(
                    await GlideJson.forget(client, "non_existing_key", {
                        path: "$",
                    }),
                ).toBe(0);
                expect(
                    await GlideJson.forget(client, "non_existing_key", {
                        path: ".",
                    }),
                ).toBe(0);
            });

            it("json.type tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = [1, 2.3, "foo", true, null, {}, []];
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.type(client, key, { path: "$[*]" }),
                ).toEqual([
                    "integer",
                    "number",
                    "string",
                    "boolean",
                    "null",
                    "object",
                    "array",
                ]);
                expect(
                    await GlideJson.type(client, "non_existing", {
                        path: "$[*]",
                    }),
                ).toBeNull();
                expect(
                    await GlideJson.type(client, key, {
                        path: "$non_existing",
                    }),
                ).toEqual([]);

                const key2 = getRandomKey();
                const jsonValue2 = { Name: "John", Age: 27 };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key2,
                        "$",
                        JSON.stringify(jsonValue2),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.type(client, key2, { path: "." }),
                ).toEqual("object");
                expect(
                    await GlideJson.type(client, key2, { path: ".Age" }),
                ).toEqual("integer");
                expect(
                    await GlideJson.type(client, key2, { path: ".Job" }),
                ).toBeNull();
                expect(
                    await GlideJson.type(client, "non_existing", { path: "." }),
                ).toBeNull();

                // Binary buffer test
                expect(
                    await GlideJson.type(client, Buffer.from(key2), {
                        path: Buffer.from(".Age"),
                    }),
                ).toEqual("integer");
            });

            it("json.clear tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    obj: { a: 1, b: 2 },
                    arr: [1, 2, 3],
                    str: "foo",
                    bool: true,
                    int: 42,
                    float: 3.14,
                    nullVal: null,
                };

                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.clear(client, key, { path: "$.*" }),
                ).toBe(6);

                const result = await GlideJson.get(client, key, {
                    path: ["$"],
                });

                expect(JSON.parse(result as string)).toEqual([
                    {
                        obj: {},
                        arr: [],
                        str: "",
                        bool: false,
                        int: 0,
                        float: 0.0,
                        nullVal: null,
                    },
                ]);

                expect(
                    await GlideJson.clear(client, key, { path: "$.*" }),
                ).toBe(0);

                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(await GlideJson.clear(client, key, { path: "*" })).toBe(
                    6,
                );

                const jsonValue2 = {
                    a: 1,
                    b: { a: [5, 6, 7], b: { a: true } },
                    c: { a: "value", b: { a: 3.5 } },
                    d: { a: { foo: "foo" } },
                    nullVal: null,
                };
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue2),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.clear(client, key, {
                        path: "b.a[1:3]",
                    }),
                ).toBe(2);

                expect(
                    await GlideJson.clear(client, key, {
                        path: "b.a[1:3]",
                    }),
                ).toBe(0);

                expect(
                    JSON.parse(
                        (await GlideJson.get(client, key, {
                            path: ["$..a"],
                        })) as string,
                    ),
                ).toEqual([1, [5, 0, 0], true, "value", 3.5, { foo: "foo" }]);

                expect(
                    await GlideJson.clear(client, key, { path: "..a" }),
                ).toBe(6);

                expect(
                    JSON.parse(
                        (await GlideJson.get(client, key, {
                            path: ["$..a"],
                        })) as string,
                    ),
                ).toEqual([0, [], false, "", 0.0, {}]);

                expect(
                    await GlideJson.clear(client, key, { path: "$..a" }),
                ).toBe(0);

                // Path doesn't exist
                expect(
                    await GlideJson.clear(client, key, { path: "$.path" }),
                ).toBe(0);

                expect(
                    await GlideJson.clear(client, key, { path: "path" }),
                ).toBe(0);

                // Key doesn't exist
                await expect(
                    GlideJson.clear(client, "non_existing_key"),
                ).rejects.toThrow(RequestError);

                await expect(
                    GlideJson.clear(client, "non_existing_key", {
                        path: "$",
                    }),
                ).rejects.toThrow(RequestError);

                await expect(
                    GlideJson.clear(client, "non_existing_key", {
                        path: ".",
                    }),
                ).rejects.toThrow(RequestError);
            });

            it("json.resp tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    obj: { a: 1, b: 2 },
                    arr: [1, 2, 3],
                    str: "foo",
                    bool: true,
                    int: 42,
                    float: 3.14,
                    nullVal: null,
                };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.resp(client, key, { path: "$.*" }),
                ).toEqual([
                    ["{", ["a", 1], ["b", 2]],
                    ["[", 1, 2, 3],
                    "foo",
                    "true",
                    42,
                    "3.14",
                    null,
                ]); // leading "{" - JSON objects, leading "[" - JSON arrays

                // multiple path match, the first will be returned
                expect(
                    await GlideJson.resp(client, key, { path: "*" }),
                ).toEqual(["{", ["a", 1], ["b", 2]]);

                // testing $ path
                expect(
                    await GlideJson.resp(client, key, { path: "$" }),
                ).toEqual([
                    [
                        "{",
                        ["obj", ["{", ["a", 1], ["b", 2]]],
                        ["arr", ["[", 1, 2, 3]],
                        ["str", "foo"],
                        ["bool", "true"],
                        ["int", 42],
                        ["float", "3.14"],
                        ["nullVal", null],
                    ],
                ]);

                // testing . path
                expect(
                    await GlideJson.resp(client, key, { path: "." }),
                ).toEqual([
                    "{",
                    ["obj", ["{", ["a", 1], ["b", 2]]],
                    ["arr", ["[", 1, 2, 3]],
                    ["str", "foo"],
                    ["bool", "true"],
                    ["int", 42],
                    ["float", "3.14"],
                    ["nullVal", null],
                ]);

                // $.str and .str
                expect(
                    await GlideJson.resp(client, key, { path: "$.str" }),
                ).toEqual(["foo"]);
                expect(
                    await GlideJson.resp(client, key, { path: ".str" }),
                ).toEqual("foo");

                // setup new json value
                const jsonValue2 = {
                    a: [1, 2, 3],
                    b: { a: [1, 2], c: { a: 42 } },
                };
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue2),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.resp(client, key, { path: "..a" }),
                ).toEqual(["[", 1, 2, 3]);

                expect(
                    await GlideJson.resp(client, key, {
                        path: "$.nonexistent",
                    }),
                ).toEqual([]);

                // error case
                await expect(
                    GlideJson.resp(client, key, { path: "nonexistent" }),
                ).rejects.toThrow(RequestError);

                // non-existent key
                expect(
                    await GlideJson.resp(client, "nonexistent_key", {
                        path: "$",
                    }),
                ).toBeNull();
                expect(
                    await GlideJson.resp(client, "nonexistent_key", {
                        path: ".",
                    }),
                ).toBeNull();
                expect(
                    await GlideJson.resp(client, "nonexistent_key"),
                ).toBeNull();

                // binary buffer test
                expect(
                    await GlideJson.resp(client, Buffer.from(key), {
                        path: Buffer.from("..a"),
                        decoder: Decoder.Bytes,
                    }),
                ).toEqual([Buffer.from("["), 1, 2, 3]);
            });

            it("json.arrtrim tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();
                const jsonValue = {
                    a: [0, 1, 2, 3, 4, 5, 6, 7, 8],
                    b: { a: [0, 9, 10, 11, 12, 13], c: { a: 42 } },
                };

                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                // Basic trim
                expect(
                    await GlideJson.arrtrim(client, key, "$..a", 1, 7),
                ).toEqual([7, 5, null]);

                // Test end >= size (should be treated as size-1)
                expect(
                    await GlideJson.arrtrim(client, key, "$.a", 0, 10),
                ).toEqual([7]);
                expect(
                    await GlideJson.arrtrim(client, key, ".a", 0, 10),
                ).toEqual(7);

                // Test negative start (should be treated as 0)
                expect(
                    await GlideJson.arrtrim(client, key, "$.a", -1, 5),
                ).toEqual([6]);
                expect(
                    await GlideJson.arrtrim(client, key, ".a", -1, 5),
                ).toEqual(6);

                // Test start >= size (should empty the array)
                expect(
                    await GlideJson.arrtrim(client, key, "$.a", 7, 10),
                ).toEqual([0]);
                const jsonValue2 = ["a", "b", "c"];
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        ".a",
                        JSON.stringify(jsonValue2),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.arrtrim(client, key, ".a", 7, 10),
                ).toEqual(0);

                // Test start > end (should empty the array)
                expect(
                    await GlideJson.arrtrim(client, key, "$..a", 2, 1),
                ).toEqual([0, 0, null]);
                const jsonValue3 = ["a", "b", "c", "d"];
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "..a",
                        JSON.stringify(jsonValue3),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.arrtrim(client, key, "..a", 2, 1),
                ).toEqual(0);

                // Multiple path match
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.arrtrim(client, key, "..a", 1, 10),
                ).toEqual(8);

                // Test with non-existent path
                await expect(
                    GlideJson.arrtrim(client, key, "nonexistent", 0, 1),
                ).rejects.toThrow(RequestError);
                expect(
                    await GlideJson.arrtrim(client, key, "$.nonexistent", 0, 1),
                ).toEqual([]);

                // Test with non-array path
                expect(await GlideJson.arrtrim(client, key, "$", 0, 1)).toEqual(
                    [null],
                );
                await expect(
                    GlideJson.arrtrim(client, key, ".", 0, 1),
                ).rejects.toThrow(RequestError);

                // Test with non-existent key
                await expect(
                    GlideJson.arrtrim(client, "non_existing_key", "$", 0, 1),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.arrtrim(client, "non_existing_key", ".", 0, 1),
                ).rejects.toThrow(RequestError);

                // Test empty array
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$.empty",
                        JSON.stringify([]),
                    ),
                ).toBe("OK");
                expect(
                    await GlideJson.arrtrim(client, key, "$.empty", 0, 1),
                ).toEqual([0]);
                expect(
                    await GlideJson.arrtrim(client, key, ".empty", 0, 1),
                ).toEqual(0);
            });

            it("json.strlen tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    a: "foo",
                    nested: { a: "hello" },
                    nested2: { a: 31 },
                };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.strlen(client, key, { path: "$..a" }),
                ).toEqual([3, 5, null]);
                expect(await GlideJson.strlen(client, key, { path: "a" })).toBe(
                    3,
                );

                expect(
                    await GlideJson.strlen(client, key, {
                        path: "$.nested",
                    }),
                ).toEqual([null]);
                expect(
                    await GlideJson.strlen(client, key, { path: "$..a" }),
                ).toEqual([3, 5, null]);

                expect(
                    await GlideJson.strlen(client, "non_existing_key", {
                        path: ".",
                    }),
                ).toBeNull();
                expect(
                    await GlideJson.strlen(client, "non_existing_key", {
                        path: "$",
                    }),
                ).toBeNull();
                expect(
                    await GlideJson.strlen(client, key, {
                        path: "$.non_existing_path",
                    }),
                ).toEqual([]);

                // error case
                await expect(
                    GlideJson.strlen(client, key, { path: "nested" }),
                ).rejects.toThrow(RequestError);
                await expect(GlideJson.strlen(client, key)).rejects.toThrow(
                    RequestError,
                );
                // Binary buffer test
                expect(
                    await GlideJson.strlen(client, Buffer.from(key), {
                        path: Buffer.from("$..a"),
                    }),
                ).toEqual([3, 5, null]);
            });

            it("json.arrappend", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                let doc = { a: 1, b: ["one", "two"] };
                expect(
                    await GlideJson.set(client, key, "$", JSON.stringify(doc)),
                ).toBe("OK");

                expect(
                    await GlideJson.arrappend(client, key, Buffer.from("$.b"), [
                        '"three"',
                    ]),
                ).toEqual([3]);
                expect(
                    await GlideJson.arrappend(client, key, ".b", [
                        '"four"',
                        '"five"',
                    ]),
                ).toEqual(5);
                doc = JSON.parse(
                    (await GlideJson.get(client, key, { path: "." })) as string,
                );
                expect(doc).toEqual({
                    a: 1,
                    b: ["one", "two", "three", "four", "five"],
                });

                expect(
                    await GlideJson.arrappend(client, key, "$.a", ['"value"']),
                ).toEqual([null]);
            });

            it("json.strappend tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    a: "foo",
                    nested: { a: "hello" },
                    nested2: { a: 31 },
                };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.strappend(client, key, '"bar"', {
                        path: "$..a",
                    }),
                ).toEqual([6, 8, null]);
                expect(
                    await GlideJson.strappend(
                        client,
                        key,
                        JSON.stringify("foo"),
                        {
                            path: "a",
                        },
                    ),
                ).toBe(9);

                expect(await GlideJson.get(client, key, { path: "." })).toEqual(
                    JSON.stringify({
                        a: "foobarfoo",
                        nested: { a: "hellobar" },
                        nested2: { a: 31 },
                    }),
                );

                // Binary buffer test
                expect(
                    await GlideJson.strappend(
                        client,
                        Buffer.from(key),
                        Buffer.from(JSON.stringify("foo")),
                        {
                            path: Buffer.from("a"),
                        },
                    ),
                ).toBe(12);

                expect(
                    await GlideJson.strappend(
                        client,
                        key,
                        JSON.stringify("bar"),
                        {
                            path: "$.nested",
                        },
                    ),
                ).toEqual([null]);

                await expect(
                    GlideJson.strappend(client, key, JSON.stringify("bar"), {
                        path: ".nested",
                    }),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.strappend(client, key, JSON.stringify("bar")),
                ).rejects.toThrow(RequestError);

                expect(
                    await GlideJson.strappend(
                        client,
                        key,
                        JSON.stringify("try"),
                        {
                            path: "$.non_existing_path",
                        },
                    ),
                ).toEqual([]);

                await expect(
                    GlideJson.strappend(client, key, JSON.stringify("try"), {
                        path: ".non_existing_path",
                    }),
                ).rejects.toThrow(RequestError);
                await expect(
                    GlideJson.strappend(
                        client,
                        "non_existing_key",
                        JSON.stringify("try"),
                    ),
                ).rejects.toThrow(RequestError);
            });

            it("json.numincrby tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    key1: 1,
                    key2: 3.5,
                    key3: { nested_key: { key1: [4, 5] } },
                    key4: [1, 2, 3],
                    key5: 0,
                    key6: "hello",
                    key7: null,
                    key8: { nested_key: { key1: 69 } },
                    key9: 1.7976931348623157e308,
                };
                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                // Increment integer value (key1) by 5
                expect(
                    await GlideJson.numincrby(client, key, "$.key1", 5),
                ).toBe("[6]"); // 1 + 5 = 6

                // Increment float value (key2) by 2.5
                expect(
                    await GlideJson.numincrby(client, key, "$.key2", 2.5),
                ).toBe("[6]"); // 3.5 + 2.5 = 6

                // Increment nested object (key3.nested_key.key1[0]) by 7
                expect(
                    await GlideJson.numincrby(
                        client,
                        key,
                        "$.key3.nested_key.key1[1]",
                        7,
                    ),
                ).toBe("[12]"); // 4 + 7 = 12

                // Increment array element (key4[1]) by 1
                expect(
                    await GlideJson.numincrby(client, key, "$.key4[1]", 1),
                ).toBe("[3]"); // 2 + 1 = 3

                // Increment zero value (key5) by 10.23 (float number)
                expect(
                    await GlideJson.numincrby(client, key, "$.key5", 10.23),
                ).toBe("[10.23]"); // 0 + 10.23 = 10.23

                // Increment a string value (key6) by a number
                expect(
                    await GlideJson.numincrby(client, key, "$.key6", 99),
                ).toBe("[null]"); // null

                // Increment a None value (key7) by a number
                expect(
                    await GlideJson.numincrby(client, key, "$.key7", 51),
                ).toBe("[null]"); // null

                // Check increment for all numbers in the document using JSON Path (First Null: key3 as an entire object. Second Null: The path checks under key3, which is an object, for numeric values).
                expect(await GlideJson.numincrby(client, key, "$..*", 5)).toBe(
                    "[11,11,null,null,15.23,null,null,null,1.7976931348623157e+308,null,null,9,17,6,8,8,null,74]",
                );

                // Check for multiple path match in enhanced
                expect(
                    await GlideJson.numincrby(client, key, "$..key1", 1),
                ).toBe("[12,null,75]");

                // Check for non existent path in JSONPath
                expect(
                    await GlideJson.numincrby(client, key, "$.key10", 51),
                ).toBe("[]"); // empty array

                // Check for non existent key in JSONPath
                await expect(
                    GlideJson.numincrby(
                        client,
                        "non_existing_key",
                        "$.key10",
                        51,
                    ),
                ).rejects.toThrow(RequestError);

                // Check for Overflow in JSONPath
                await expect(
                    GlideJson.numincrby(
                        client,
                        key,
                        "$.key9",
                        1.7976931348623157e308,
                    ),
                ).rejects.toThrow(RequestError);

                // Decrement integer value (key1) by 12
                expect(
                    await GlideJson.numincrby(client, key, "$.key1", -12),
                ).toBe("[0]"); // 12 - 12 = 0

                // Decrement integer value (key1) by 0.5
                expect(
                    await GlideJson.numincrby(client, key, "$.key1", -0.5),
                ).toBe("[-0.5]"); // 0 - 0.5 = -0.5

                // Test Legacy Path
                // Increment float value (key1) by 5 (integer)
                expect(await GlideJson.numincrby(client, key, "key1", 5)).toBe(
                    "4.5",
                ); // -0.5 + 5 = 4.5

                // Decrement float value (key1) by 5.5 (integer)
                expect(
                    await GlideJson.numincrby(client, key, "key1", -5.5),
                ).toBe("-1"); // 4.5 - 5.5 = -1

                // Increment int value (key2) by 2.5 (a float number)
                expect(
                    await GlideJson.numincrby(client, key, "key2", 2.5),
                ).toBe("13.5"); // 11 + 2.5 = 13.5

                // Increment nested value (key3.nested_key.key1[0]) by 7
                expect(
                    await GlideJson.numincrby(
                        client,
                        key,
                        "key3.nested_key.key1[0]",
                        7,
                    ),
                ).toBe("16"); // 9 + 7 = 16

                // Increment array element (key4[1]) by 1
                expect(
                    await GlideJson.numincrby(client, key, "key4[1]", 1),
                ).toBe("9"); // 8 + 1 = 9

                // Increment a float value (key5) by 10.2 (a float number)
                expect(
                    await GlideJson.numincrby(client, key, "key5", 10.2),
                ).toBe("25.43"); // 15.23 + 10.2 = 25.43

                // Check for multiple path match in legacy and assure that the result of the last updated value is returned
                expect(
                    await GlideJson.numincrby(client, key, "..key1", 1),
                ).toBe("76");

                // Check if the rest of the key1 path matches were updated and not only the last value
                expect(
                    await GlideJson.get(client, key, { path: "$..key1" }),
                ).toBe("[0,[16,17],76]");
                // First is 0 as 0 + 0 = 0, Second doesn't change as its an array type (non-numeric), third is 76 as 0 + 76 = 0

                // Check for non existent path in legacy
                await expect(
                    GlideJson.numincrby(client, key, ".key10", 51),
                ).rejects.toThrow(RequestError);

                // Check for non existent key in legacy
                await expect(
                    GlideJson.numincrby(
                        client,
                        "non_existent_key",
                        ".key10",
                        51,
                    ),
                ).rejects.toThrow(RequestError);

                // Check for Overflow in legacy
                await expect(
                    GlideJson.numincrby(
                        client,
                        key,
                        ".key9",
                        1.7976931348623157e308,
                    ),
                ).rejects.toThrow(RequestError);

                // binary buffer test
                expect(
                    await GlideJson.numincrby(
                        client,
                        Buffer.from(key),
                        Buffer.from("key5"),
                        1,
                    ),
                ).toBe("26.43");
            });

            it("json.nummultiby tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue =
                    "{" +
                    ' "key1": 1,' +
                    ' "key2": 3.5,' +
                    ' "key3": {"nested_key": {"key1": [4, 5]}},' +
                    ' "key4": [1, 2, 3],' +
                    ' "key5": 0,' +
                    ' "key6": "hello",' +
                    ' "key7": null,' +
                    ' "key8": {"nested_key": {"key1": 69}},' +
                    ' "key9": 3.5953862697246314e307' +
                    "}";
                // setup
                expect(await GlideJson.set(client, key, "$", jsonValue)).toBe(
                    "OK",
                );

                // Test JSONPath
                // Multiply integer value (key1) by 5
                expect(
                    await GlideJson.nummultby(client, key, "$.key1", 5),
                ).toBe("[5]"); //  1 * 5 = 5

                // Multiply float value (key2) by 2.5
                expect(
                    await GlideJson.nummultby(client, key, "$.key2", 2.5),
                ).toBe("[8.75]"); //  3.5 * 2.5 = 8.75

                // Multiply nested object (key3.nested_key.key1[1]) by 7
                expect(
                    await GlideJson.nummultby(
                        client,
                        key,
                        "$.key3.nested_key.key1[1]",
                        7,
                    ),
                ).toBe("[35]"); //  5 * 7 = 5

                // Multiply array element (key4[1]) by 1
                expect(
                    await GlideJson.nummultby(client, key, "$.key4[1]", 1),
                ).toBe("[2]"); //  2 * 1 = 2

                // Multiply zero value (key5) by 10.23 (float number)
                expect(
                    await GlideJson.nummultby(client, key, "$.key5", 10.23),
                ).toBe("[0]"); // 0 * 10.23 = 0

                // Multiply a string value (key6) by a number
                expect(
                    await GlideJson.nummultby(client, key, "$.key6", 99),
                ).toBe("[null]");

                // Multiply a None value (key7) by a number
                expect(
                    await GlideJson.nummultby(client, key, "$.key7", 51),
                ).toBe("[null]");

                // Check multiplication for all numbers in the document using JSON Path
                // key1: 5 * 5 = 25
                // key2: 8.75 * 5 = 43.75
                // key3.nested_key.key1[0]: 4 * 5 = 20
                // key3.nested_key.key1[1]: 35 * 5 = 175
                // key4[0]: 1 * 5 = 5
                // key4[1]: 2 * 5 = 10
                // key4[2]: 3 * 5 = 15
                // key5: 0 * 5 = 0
                // key8.nested_key.key1: 69 * 5 = 345
                // key9: 3.5953862697246314e307 * 5 = 1.7976931348623157e308
                expect(await GlideJson.nummultby(client, key, "$..*", 5)).toBe(
                    "[25,43.75,null,null,0,null,null,null,1.7976931348623157e+308,null,null,20,175,5,10,15,null,345]",
                );

                // Check for multiple path matches in JSONPath
                // key1: 25 * 2 = 50
                // key8.nested_key.key1: 345 * 2 = 690
                expect(
                    await GlideJson.nummultby(client, key, "$..key1", 2),
                ).toBe("[50,null,690]"); //  After previous multiplications

                // Check for non-existent path in JSONPath
                expect(
                    await GlideJson.nummultby(client, key, "$.key10", 51),
                ).toBe("[]"); //  Empty Array

                // Check for non-existent key in JSONPath
                await expect(
                    GlideJson.numincrby(
                        client,
                        "non_existent_key",
                        "$.key10",
                        51,
                    ),
                ).rejects.toThrow(RequestError);

                // Check for Overflow in JSONPath
                await expect(
                    GlideJson.numincrby(
                        client,
                        key,
                        "$.key9",
                        1.7976931348623157e308,
                    ),
                ).rejects.toThrow(RequestError);

                // Multiply integer value (key1) by -12
                expect(
                    await GlideJson.nummultby(client, key, "$.key1", -12),
                ).toBe("[-600]"); // 50 * -12 = -600

                // Multiply integer value (key1) by -0.5
                expect(
                    await GlideJson.nummultby(client, key, "$.key1", -0.5),
                ).toBe("[300]"); //  -600 * -0.5 = 300

                // Test Legacy Path
                // Multiply int value (key1) by 5 (integer)
                expect(await GlideJson.nummultby(client, key, "key1", 5)).toBe(
                    "1500",
                ); //  300 * 5 = -1500

                // Multiply int value (key1) by -5.5 (float number)
                expect(
                    await GlideJson.nummultby(client, key, "key1", -5.5),
                ).toBe("-8250"); //  -150 * -5.5 = -8250

                // Multiply int float (key2) by 2.5 (a float number)
                expect(
                    await GlideJson.nummultby(client, key, "key2", 2.5),
                ).toBe("109.375"); // 109.375

                // Multiply nested value (key3.nested_key.key1[0]) by 7
                expect(
                    await GlideJson.nummultby(
                        client,
                        key,
                        "key3.nested_key.key1[0]",
                        7,
                    ),
                ).toBe("140"); // 20 * 7 = 140

                // Multiply array element (key4[1]) by 1
                expect(
                    await GlideJson.nummultby(client, key, "key4[1]", 1),
                ).toBe("10"); //  10 * 1 = 10

                // Multiply a float value (key5) by 10.2 (a float number)
                expect(
                    await GlideJson.nummultby(client, key, "key5", 10.2),
                ).toBe("0"); // 0 * 10.2 = 0

                // Check for multiple path matches in legacy and assure that the result of the last updated value is returned
                // last updated value is key8.nested_key.key1: 690 * 2 = 1380
                expect(
                    await GlideJson.nummultby(client, key, "..key1", 2),
                ).toBe("1380"); //  the last updated key1 value multiplied by 2

                // Check if the rest of the key1 path matches were updated and not only the last value
                expect(
                    await GlideJson.get(client, key, { path: "$..key1" }),
                ).toBe("[-16500,[140,175],1380]");

                // Check for non-existent path in legacy
                await expect(
                    GlideJson.numincrby(client, key, ".key10", 51),
                ).rejects.toThrow(RequestError);

                // Check for non-existent key in legacy
                await expect(
                    GlideJson.numincrby(
                        client,
                        "non_existent_key",
                        ".key10",
                        51,
                    ),
                ).rejects.toThrow(RequestError);

                // Check for Overflow in legacy
                await expect(
                    GlideJson.numincrby(
                        client,
                        key,
                        ".key9",
                        1.7976931348623157e308,
                    ),
                ).rejects.toThrow(RequestError);

                // binary buffer tests
                expect(
                    await GlideJson.nummultby(
                        client,
                        Buffer.from(key),
                        Buffer.from("key5"),
                        10.2,
                    ),
                ).toBe("0"); // 0 * 10.2 = 0
            });

            it("json.debug tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue =
                    '{ "key1": 1, "key2": 3.5, "key3": {"nested_key": {"key1": [4, 5]}}, "key4":' +
                    ' [1, 2, 3], "key5": 0, "key6": "hello", "key7": null, "key8":' +
                    ' {"nested_key": {"key1": 3.5953862697246314e307}}, "key9":' +
                    ' 3.5953862697246314e307, "key10": true }';
                // setup
                expect(await GlideJson.set(client, key, "$", jsonValue)).toBe(
                    "OK",
                );

                expect(
                    await GlideJson.debugFields(client, key, {
                        path: "$.key1",
                    }),
                ).toEqual([1]);

                expect(
                    await GlideJson.debugFields(client, key, {
                        path: "$.key3.nested_key.key1",
                    }),
                ).toEqual([2]);

                expect(
                    await GlideJson.debugMemory(client, key, {
                        path: "$.key4[2]",
                    }),
                ).toEqual([16]);

                expect(
                    await GlideJson.debugMemory(client, key, {
                        path: ".key6",
                    }),
                ).toEqual(16);

                expect(await GlideJson.debugMemory(client, key)).toEqual(504);

                expect(await GlideJson.debugFields(client, key)).toEqual(19);

                // testing binary input
                expect(
                    await GlideJson.debugMemory(client, Buffer.from(key)),
                ).toEqual(504);

                expect(
                    await GlideJson.debugFields(client, Buffer.from(key)),
                ).toEqual(19);
            });

            it("json.objlen tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    a: 1.0,
                    b: { a: { x: 1, y: 2 }, b: 2.5, c: true },
                };

                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.objlen(client, key, { path: "$" }),
                ).toEqual([2]);

                expect(
                    await GlideJson.objlen(client, key, { path: "." }),
                ).toEqual(2);

                expect(
                    await GlideJson.objlen(client, key, { path: "$.." }),
                ).toEqual([2, 3, 2]);

                expect(
                    await GlideJson.objlen(client, key, { path: ".." }),
                ).toEqual(2);

                expect(
                    await GlideJson.objlen(client, key, { path: "$..b" }),
                ).toEqual([3, null]);

                expect(
                    await GlideJson.objlen(client, key, { path: "..b" }),
                ).toEqual(3);

                expect(
                    await GlideJson.objlen(client, Buffer.from(key), {
                        path: Buffer.from("..a"),
                    }),
                ).toEqual(2);

                expect(await GlideJson.objlen(client, key)).toEqual(2);

                // path doesn't exist
                expect(
                    await GlideJson.objlen(client, key, {
                        path: "$.non_existing_path",
                    }),
                ).toEqual([]);

                await expect(
                    GlideJson.objlen(client, key, {
                        path: "non_existing_path",
                    }),
                ).rejects.toThrow(RequestError);

                // Value at path isnt an object
                expect(
                    await GlideJson.objlen(client, key, {
                        path: "$.non_existing_path",
                    }),
                ).toEqual([]);

                await expect(
                    GlideJson.objlen(client, key, { path: ".a" }),
                ).rejects.toThrow(RequestError);

                // Non-existing key
                expect(
                    await GlideJson.objlen(client, "non_existing_key", {
                        path: "$",
                    }),
                ).toBeNull();

                expect(
                    await GlideJson.objlen(client, "non_existing_key", {
                        path: ".",
                    }),
                ).toBeNull();

                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        '{"a": 1, "b": 2, "c":3, "d":4}',
                    ),
                ).toBe("OK");
                expect(await GlideJson.objlen(client, key)).toEqual(4);
            });

            it("json.objkeys tests", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const key = getRandomKey();
                const jsonValue = {
                    a: 1.0,
                    b: { a: { x: 1, y: 2 }, b: 2.5, c: true },
                };

                // setup
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify(jsonValue),
                    ),
                ).toBe("OK");

                expect(
                    await GlideJson.objkeys(client, key, { path: "$" }),
                ).toEqual([["a", "b"]]);

                expect(
                    await GlideJson.objkeys(client, key, {
                        path: ".",
                        decoder: Decoder.Bytes,
                    }),
                ).toEqual([Buffer.from("a"), Buffer.from("b")]);

                expect(
                    await GlideJson.objkeys(client, Buffer.from(key), {
                        path: Buffer.from("$.."),
                    }),
                ).toEqual([
                    ["a", "b"],
                    ["a", "b", "c"],
                    ["x", "y"],
                ]);

                expect(
                    await GlideJson.objkeys(client, key, { path: ".." }),
                ).toEqual(["a", "b"]);

                expect(
                    await GlideJson.objkeys(client, key, { path: "$..b" }),
                ).toEqual([["a", "b", "c"], []]);

                expect(
                    await GlideJson.objkeys(client, key, { path: "..b" }),
                ).toEqual(["a", "b", "c"]);

                // path doesn't exist
                expect(
                    await GlideJson.objkeys(client, key, {
                        path: "$.non_existing_path",
                    }),
                ).toEqual([]);

                expect(
                    await GlideJson.objkeys(client, key, {
                        path: "non_existing_path",
                    }),
                ).toBeNull();

                // Value at path isnt an object
                expect(
                    await GlideJson.objkeys(client, key, { path: "$.a" }),
                ).toEqual([[]]);

                await expect(
                    GlideJson.objkeys(client, key, { path: ".a" }),
                ).rejects.toThrow(RequestError);

                // Non-existing key
                expect(
                    await GlideJson.objkeys(client, "non_existing_key", {
                        path: "$",
                    }),
                ).toBeNull();

                expect(
                    await GlideJson.objkeys(client, "non_existing_key", {
                        path: ".",
                    }),
                ).toBeNull();
            });

            it("json.mset", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key1 = getRandomKey();
                const key2 = getRandomKey();

                // Set multiple keys in one call
                expect(
                    await GlideJson.mset(client, [
                        {
                            key: key1,
                            path: "$",
                            value: JSON.stringify({ a: 1, b: "hello" }),
                        },
                        {
                            key: key2,
                            path: "$",
                            value: JSON.stringify({ c: 2, d: "world" }),
                        },
                    ]),
                ).toBe("OK");

                // Verify values via json.get
                let result1 = await GlideJson.get(client, key1, {
                    path: "$.a",
                });
                expect(JSON.parse(result1 as string)).toEqual([1]);

                let result2 = await GlideJson.get(client, key2, {
                    path: "$.d",
                });
                expect(JSON.parse(result2 as string)).toEqual(["world"]);

                // Set multiple paths on same key
                expect(
                    await GlideJson.mset(client, [
                        {
                            key: key1,
                            path: "$.a",
                            value: "42",
                        },
                        {
                            key: key1,
                            path: "$.b",
                            value: '"updated"',
                        },
                    ]),
                ).toBe("OK");

                // Verify updated values
                result1 = await GlideJson.get(client, key1, {
                    path: "$.a",
                });
                expect(JSON.parse(result1 as string)).toEqual([42]);

                result1 = await GlideJson.get(client, key1, {
                    path: "$.b",
                });
                expect(JSON.parse(result1 as string)).toEqual(["updated"]);

                await client.del([key1, key2]);
            });

            it.each([true, false])(
                "can send JsonBatch batches for ARR commands with isAtomic=%s",
                async (isAtomic) => {
                    client = await GlideClusterClient.createClient(
                        getClientConfigurationOption(
                            cluster.getAddresses(),
                            protocol,
                        ),
                    );
                    const batch = new ClusterBatch(isAtomic);
                    const expectedRes = await JsonBatchForArrCommands(batch);
                    const result = await client.exec(batch, true);

                    validateBatchResponse(result, expectedRes);
                    client.close();
                },
            );

            it.each([true, false])(
                "can send JsonBatch batches general commands with isAtomic=%s",
                async (isAtomic) => {
                    client = await GlideClusterClient.createClient(
                        getClientConfigurationOption(
                            cluster.getAddresses(),
                            protocol,
                        ),
                    );
                    const batch = new ClusterBatch(isAtomic);
                    const expectedRes = await CreateJsonBatchCommands(batch);
                    const result = await client.exec(batch, true);

                    validateBatchResponse(result, expectedRes);
                    client.close();
                },
            );

            // --- Edge case tests: json.mset ---

            it("json.mset overwrites existing values", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();

                // Set initial value
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify({ a: 1, b: "old" }),
                    ),
                ).toBe("OK");

                // Overwrite with mset
                expect(
                    await GlideJson.mset(client, [
                        {
                            key,
                            path: "$",
                            value: JSON.stringify({ a: 99, b: "new" }),
                        },
                    ]),
                ).toBe("OK");

                const result = await GlideJson.get(client, key, {
                    path: "$",
                });
                expect(JSON.parse(result as string)).toEqual([
                    { a: 99, b: "new" },
                ]);

                await client.del([key]);
            });

            it("json.mset with nested JSON paths", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const key = getRandomKey();

                // Create a nested document first
                expect(
                    await GlideJson.set(
                        client,
                        key,
                        "$",
                        JSON.stringify({
                            outer: { inner: { value: "original" } },
                        }),
                    ),
                ).toBe("OK");

                // Set nested path via mset
                expect(
                    await GlideJson.mset(client, [
                        {
                            key,
                            path: "$.outer.inner.value",
                            value: '"updated"',
                        },
                    ]),
                ).toBe("OK");

                const result = await GlideJson.get(client, key, {
                    path: "$.outer.inner.value",
                });
                expect(JSON.parse(result as string)).toEqual(["updated"]);

                await client.del([key]);
            });

            it("json.mset large batch (50 keys)", async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const keys: string[] = [];
                const entries: {
                    key: string;
                    path: string;
                    value: string;
                }[] = [];

                for (let i = 0; i < 50; i++) {
                    const k = getRandomKey();
                    keys.push(k);
                    entries.push({
                        key: k,
                        path: "$",
                        value: JSON.stringify({ idx: i }),
                    });
                }

                expect(await GlideJson.mset(client, entries)).toBe("OK");

                // Verify a sample of keys
                for (const idx of [0, 24, 49]) {
                    const result = await GlideJson.get(client, keys[idx], {
                        path: "$.idx",
                    });
                    expect(JSON.parse(result as string)).toEqual([idx]);
                }

                await client.del(keys);
            });
        },
    );

    describe("GlideFt", () => {
        let client: GlideClusterClient;

        afterEach(async () => {
            await flushAndCloseClient(true, cluster?.getAddresses(), client);
        });

        it("ServerModules check Vector Search module is loaded", async () => {
            client = await GlideClusterClient.createClient(
                getClientConfigurationOption(
                    cluster.getAddresses(),
                    ProtocolVersion.RESP3,
                ),
            );
            const info = await client.info({
                sections: [InfoOptions.Modules],
                route: "randomNode",
            });
            expect(info).toContain("# search_index_stats");
        });

        it("FT.CREATE test", async () => {
            client = await GlideClusterClient.createClient(
                getClientConfigurationOption(
                    cluster.getAddresses(),
                    ProtocolVersion.RESP3,
                ),
            );

            // Create a few simple indices:
            const vectorField_1: VectorField = {
                type: "VECTOR",
                name: "vec",
                alias: "VEC",
                attributes: {
                    algorithm: "HNSW",
                    type: "FLOAT32",
                    dimensions: 2,
                    distanceMetric: "L2",
                },
            };
            expect(
                await GlideFt.create(client, getRandomKey(), [vectorField_1]),
            ).toEqual("OK");

            expect(
                await GlideFt.create(
                    client,
                    "json_idx1",
                    [
                        {
                            type: "VECTOR",
                            name: "$.vec",
                            alias: "VEC",
                            attributes: {
                                algorithm: "HNSW",
                                type: "FLOAT32",
                                dimensions: 6,
                                distanceMetric: "L2",
                                numberOfEdges: 32,
                            },
                        },
                    ],
                    {
                        dataType: "JSON",
                        prefixes: ["json:"],
                    },
                ),
            ).toEqual("OK");

            const vectorField_2: VectorField = {
                type: "VECTOR",
                name: "$.vec",
                alias: "VEC",
                attributes: {
                    algorithm: "FLAT",
                    type: "FLOAT32",
                    dimensions: 6,
                    distanceMetric: "L2",
                },
            };
            expect(
                await GlideFt.create(client, getRandomKey(), [vectorField_2]),
            ).toEqual("OK");

            // create an index with HNSW vector with additional parameters
            const vectorField_3: VectorField = {
                type: "VECTOR",
                name: "doc_embedding",
                attributes: {
                    algorithm: "HNSW",
                    type: "FLOAT32",
                    dimensions: 1536,
                    distanceMetric: "COSINE",
                    numberOfEdges: 40,
                    vectorsExaminedOnConstruction: 250,
                    vectorsExaminedOnRuntime: 40,
                },
            };
            expect(
                await GlideFt.create(client, getRandomKey(), [vectorField_3], {
                    dataType: "HASH",
                    prefixes: ["docs:"],
                }),
            ).toEqual("OK");

            // create an index with multiple fields
            expect(
                await GlideFt.create(
                    client,
                    getRandomKey(),
                    [
                        { type: "TEXT", name: "title" },
                        { type: "NUMERIC", name: "published_at" },
                        { type: "TAG", name: "category" },
                    ],
                    { dataType: "HASH", prefixes: ["blog:post:"] },
                ),
            ).toEqual("OK");

            // create an index with multiple prefixes
            const name = getRandomKey();
            expect(
                await GlideFt.create(
                    client,
                    name,
                    [
                        { type: "TAG", name: "author_id" },
                        { type: "TAG", name: "author_ids" },
                        { type: "TEXT", name: "title" },
                        { type: "TEXT", name: "name" },
                    ],
                    {
                        dataType: "HASH",
                        prefixes: ["author:details:", "book:details:"],
                    },
                ),
            ).toEqual("OK");

            // create a duplicating index - expect a RequestError
            try {
                expect(
                    await GlideFt.create(client, name, [
                        { type: "TEXT", name: "title" },
                        { type: "TEXT", name: "name" },
                    ]),
                ).rejects.toThrow();
            } catch (e) {
                expect((e as Error).message).toContain("already exists");
            }

            // create an index without fields - expect a RequestError
            try {
                expect(
                    await GlideFt.create(client, getRandomKey(), []),
                ).rejects.toThrow();
            } catch (e) {
                expect((e as Error).message).toContain(
                    "wrong number of arguments",
                );
            }

            // duplicated field name - expect a RequestError
            try {
                expect(
                    await GlideFt.create(client, getRandomKey(), [
                        { type: "TEXT", name: "name" },
                        { type: "TEXT", name: "name" },
                    ]),
                ).rejects.toThrow();
            } catch (e) {
                expect((e as Error).message).toContain("already exists");
            }
        });

        it("FT.DROPINDEX FT._LIST FT.LIST", async () => {
            client = await GlideClusterClient.createClient(
                getClientConfigurationOption(
                    cluster.getAddresses(),
                    ProtocolVersion.RESP3,
                ),
            );

            // create an index
            const index = getRandomKey();
            expect(
                await GlideFt.create(client, index, [
                    {
                        type: "VECTOR",
                        name: "vec",
                        attributes: {
                            algorithm: "HNSW",
                            distanceMetric: "L2",
                            dimensions: 2,
                        },
                    },
                    { type: "NUMERIC", name: "published_at" },
                    { type: "TAG", name: "category" },
                ]),
            ).toEqual("OK");

            const before = await GlideFt.list(client);
            expect(before).toContain(index);

            // DROP it
            expect(await GlideFt.dropindex(client, index)).toEqual("OK");

            const after = await GlideFt.list(client);
            expect(after).not.toContain(index);

            // dropping the index again results in an error
            try {
                expect(
                    await GlideFt.dropindex(client, index),
                ).rejects.toThrow();
            } catch (e) {
                expect((e as Error).message).toContain("Index does not exist");
            }
        });

        it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
            "FT.INFO ft.info",
            async (protocol) => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const index = getRandomKey();
                expect(
                    await GlideFt.create(
                        client,
                        Buffer.from(index),
                        [
                            {
                                type: "VECTOR",
                                name: "$.vec",
                                alias: "VEC",
                                attributes: {
                                    algorithm: "HNSW",
                                    distanceMetric: "COSINE",
                                    dimensions: 42,
                                },
                            },
                            { type: "TEXT", name: "$.name" },
                        ],
                        { dataType: "JSON", prefixes: ["123"] },
                    ),
                ).toEqual("OK");

                let response = await GlideFt.info(client, Buffer.from(index));

                expect(response).toMatchObject({
                    index_name: index,
                    key_type: "JSON",
                    key_prefixes: ["123"],
                    fields: [
                        {
                            identifier: "$.name",
                            type: "TEXT",
                            field_name: "$.name",
                            option: "",
                        },
                        {
                            identifier: "$.vec",
                            type: "VECTOR",
                            field_name: "VEC",
                            option: "",
                            vector_params: {
                                distance_metric: "COSINE",
                                dimension: 42,
                            },
                        },
                    ],
                });

                response = await GlideFt.info(client, index, {
                    decoder: Decoder.Bytes,
                });
                expect(response).toMatchObject({
                    index_name: Buffer.from(index),
                });

                expect(await GlideFt.dropindex(client, index)).toEqual("OK");
                // querying a missing index
                await expect(GlideFt.info(client, index)).rejects.toThrow(
                    "Index not found",
                );
            },
        );

        it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
            "FT.SEARCH binary on HASH",
            async (protocol) => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );
                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "index";
                const query = "*=>[KNN 2 @VEC $query_vec]";

                // setup a hash index:
                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "VECTOR",
                                name: "vec",
                                alias: "VEC",
                                attributes: {
                                    algorithm: "HNSW",
                                    distanceMetric: "L2",
                                    dimensions: 2,
                                },
                            },
                        ],
                        {
                            dataType: "HASH",
                            prefixes: [prefix],
                        },
                    ),
                ).toEqual("OK");

                const binaryValue1 = Buffer.alloc(8);
                expect(
                    await client.hset(Buffer.from(prefix + "0"), [
                        // value of <Buffer 00 00 00 00 00 00 00 00 00>
                        { field: "vec", value: binaryValue1 },
                    ]),
                ).toEqual(1);

                const binaryValue2: Buffer = Buffer.alloc(8);
                binaryValue2[6] = 0x80;
                binaryValue2[7] = 0xbf;
                expect(
                    await client.hset(Buffer.from(prefix + "1"), [
                        // value of <Buffer 00 00 00 00 00 00 00 80 BF>
                        { field: "vec", value: binaryValue2 },
                    ]),
                ).toEqual(1);

                // let server digest the data and update index
                const sleep = new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );
                await sleep;

                // With the `COUNT` parameters - returns only the count
                const optionsWithCount: FtSearchOptions = {
                    params: [{ key: "query_vec", value: binaryValue1 }],
                    timeout: 10000,
                    count: true,
                };
                const binaryResultCount: FtSearchReturnType =
                    await GlideFt.search(client, index, query, {
                        decoder: Decoder.Bytes,
                        ...optionsWithCount,
                    });
                expect(binaryResultCount).toEqual([2]);

                const options: FtSearchOptions = {
                    params: [{ key: "query_vec", value: binaryValue1 }],
                    timeout: 10000,
                };
                const binaryResult: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    query,
                    {
                        decoder: Decoder.Bytes,
                        ...options,
                    },
                );

                const expectedBinaryResult: FtSearchReturnType = [
                    2,
                    [
                        {
                            key: Buffer.from(prefix + "1"),
                            value: [
                                {
                                    key: Buffer.from("vec"),
                                    value: binaryValue2,
                                },
                                {
                                    key: Buffer.from("__VEC_score"),
                                    value: Buffer.from("1"),
                                },
                            ],
                        },
                        {
                            key: Buffer.from(prefix + "0"),
                            value: [
                                {
                                    key: Buffer.from("vec"),
                                    value: binaryValue1,
                                },
                                {
                                    key: Buffer.from("__VEC_score"),
                                    value: Buffer.from("0"),
                                },
                            ],
                        },
                    ],
                ];
                expect(binaryResult).toEqual(expectedBinaryResult);
            },
        );

        it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
            "FT.SEARCH binary on JSON",
            async (protocol) => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        protocol,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "index";
                const query = "*";

                // set string values
                expect(
                    await GlideJson.set(
                        client,
                        prefix + "1",
                        "$",
                        '[{"arr": 42}, {"val": "hello"}, {"val": "world"}]',
                    ),
                ).toEqual("OK");

                // setup a json index:
                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "NUMERIC",
                                name: "$..arr",
                                alias: "arr",
                            },
                            {
                                type: "TEXT",
                                name: "$..val",
                                alias: "val",
                            },
                        ],
                        {
                            dataType: "JSON",
                            prefixes: [prefix],
                        },
                    ),
                ).toEqual("OK");

                // let server digest the data and update index
                const sleep = new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );
                await sleep;

                const optionsWithLimit: FtSearchOptions = {
                    returnFields: [
                        { fieldIdentifier: "$..arr", alias: "myarr" },
                        { fieldIdentifier: "$..val", alias: "myval" },
                    ],
                    timeout: 10000,
                    limit: { offset: 0, count: 2 },
                };
                const stringResult: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    query,
                    optionsWithLimit,
                );
                const expectedStringResult: FtSearchReturnType = [
                    1,
                    [
                        {
                            key: prefix + "1",
                            value: [
                                {
                                    key: "myarr",
                                    value: "42",
                                },
                                {
                                    key: "myval",
                                    value: "hello",
                                },
                            ],
                        },
                    ],
                ];
                expect(stringResult).toEqual(expectedStringResult);
            },
        );

        it(
            "FT.CREATE with sortable field",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const index = getRandomKey();
                const prefix = "{" + getRandomKey() + "}:";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "TAG",
                                name: "category",
                                sortable: true,
                            },
                            { type: "TEXT", name: "title" },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                const info = await GlideFt.info(client, index);
                expect(info).toBeTruthy();
                expect(info["index_name"]).toEqual(index);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.CREATE with skipInitialScan",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                // Add data before creating index
                await client.hset(prefix + "1", {
                    title: "hello",
                    category: "greeting",
                });

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            { type: "TAG", name: "category" },
                            { type: "TEXT", name: "title" },
                        ],
                        {
                            dataType: "HASH",
                            prefixes: [prefix],
                            skipInitialScan: true,
                        },
                    ),
                ).toEqual("OK");

                const info = await GlideFt.info(client, index);
                expect(info).toBeTruthy();

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH with nocontent",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [{ type: "TEXT", name: "title" }],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await client.hset(prefix + "1", { title: "hello world" });
                await client.hset(prefix + "2", { title: "goodbye world" });

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    { nocontent: true },
                );
                // nocontent returns only count and keys without field values
                expect(result[0]).toEqual(2);

                if (result.length > 1) {
                    for (const doc of result[1]) {
                        // Each doc should have a key but an empty value array
                        expect(doc.key).toBeTruthy();
                        expect(doc.value).toEqual([]);
                    }
                }

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH with sortby",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            { type: "TEXT", name: "title", sortable: true },
                            { type: "NUMERIC", name: "price", sortable: true },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await client.hset(prefix + "1", {
                    title: "banana",
                    price: "30",
                });
                await client.hset(prefix + "2", {
                    title: "apple",
                    price: "10",
                });
                await client.hset(prefix + "3", {
                    title: "cherry",
                    price: "20",
                });

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // Sort by price ascending
                const resultAsc: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    {
                        sortby: {
                            field: "price",
                            order: SortOrder.ASC,
                        },
                    },
                );
                expect(resultAsc[0]).toEqual(3);
                // Verify ordering: price 10, 20, 30
                expect(resultAsc[1][0].key).toContain("2"); // apple, price 10
                expect(resultAsc[1][1].key).toContain("3"); // cherry, price 20
                expect(resultAsc[1][2].key).toContain("1"); // banana, price 30

                // Sort by price descending
                const resultDesc: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    {
                        sortby: {
                            field: "price",
                            order: SortOrder.DESC,
                        },
                    },
                );
                expect(resultDesc[0]).toEqual(3);
                expect(resultDesc[1][0].key).toContain("1"); // banana, price 30
                expect(resultDesc[1][1].key).toContain("3"); // cherry, price 20
                expect(resultDesc[1][2].key).toContain("2"); // apple, price 10

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH with dialect",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [{ type: "TEXT", name: "title" }],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await client.hset(prefix + "1", { title: "hello world" });

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // Dialect 2 is the default for valkey-search
                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    { dialect: 2 },
                );
                expect(result[0]).toEqual(1);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        // --- Edge case tests: FT.SEARCH options ---

        it(
            "FT.SEARCH with limit offset=0 count=0 returns empty",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [{ type: "TEXT", name: "title" }],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await client.hset(prefix + "1", { title: "hello" });

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // count=0 should return total count but no documents
                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    { limit: { offset: 0, count: 0 } },
                );
                // Total count should still reflect matching docs
                expect(result[0]).toEqual(1);
                // But no documents should be returned
                expect(result[1]).toEqual([]);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH with very large limit returns all available",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [{ type: "TEXT", name: "title" }],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await client.hset(prefix + "1", { title: "doc one" });
                await client.hset(prefix + "2", { title: "doc two" });
                await client.hset(prefix + "3", { title: "doc three" });

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // Requesting a very large count should return all available
                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                    { limit: { offset: 0, count: 100000 } },
                );
                expect(result[0]).toEqual(3);
                expect(result[1]).toHaveLength(3);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.CREATE on prefix with no matching keys returns 0 results",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:nomatch:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [{ type: "TEXT", name: "title" }],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // No keys match this prefix, so search returns 0
                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*",
                );
                expect(result[0]).toEqual(0);
                expect(result[1]).toEqual([]);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.DROPINDEX on non-existent index throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                await expect(
                    GlideFt.dropindex(client, "nonexistent_index_" + getRandomKey()),
                ).rejects.toThrow(RequestError);
            },
            TIMEOUT,
        );

        it(
            "FT.CREATE with duplicate index name throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const index = getRandomKey();

                expect(
                    await GlideFt.create(client, index, [
                        { type: "TEXT", name: "title" },
                    ]),
                ).toEqual("OK");

                // Creating the same index again should throw
                await expect(
                    GlideFt.create(client, index, [
                        { type: "TEXT", name: "body" },
                    ]),
                ).rejects.toThrow(RequestError);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.CREATE with duplicate field names throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                await expect(
                    GlideFt.create(client, getRandomKey(), [
                        { type: "TEXT", name: "samename" },
                        { type: "TEXT", name: "samename" },
                    ]),
                ).rejects.toThrow(RequestError);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH with nocontent and KNN returns keys only",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "VECTOR",
                                name: "vec",
                                alias: "VEC",
                                attributes: {
                                    algorithm: "FLAT",
                                    distanceMetric: "L2",
                                    dimensions: 2,
                                },
                            },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                const vec1 = Buffer.alloc(8);
                const vec2 = Buffer.alloc(8);
                vec2.writeFloatLE(1.0, 0);
                vec2.writeFloatLE(1.0, 4);

                await client.hset(Buffer.from(prefix + "a"), [
                    { field: "vec", value: vec1 },
                ]);
                await client.hset(Buffer.from(prefix + "b"), [
                    { field: "vec", value: vec2 },
                ]);

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*=>[KNN 2 @VEC $query_vec]",
                    {
                        params: [{ key: "query_vec", value: vec1 }],
                        nocontent: true,
                    },
                );

                // nocontent should return count and keys with empty value arrays
                expect(result[0]).toEqual(2);

                if (result.length > 1) {
                    for (const doc of result[1]) {
                        expect(doc.key).toBeTruthy();
                        expect(doc.value).toEqual([]);
                    }
                }

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH vector search with mismatched dimensions throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                // Create index with 2-dimensional vectors
                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "VECTOR",
                                name: "vec",
                                alias: "VEC",
                                attributes: {
                                    algorithm: "FLAT",
                                    distanceMetric: "L2",
                                    dimensions: 2,
                                },
                            },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                // Insert a correct 2D vector
                const vec2d = Buffer.alloc(8);
                await client.hset(Buffer.from(prefix + "1"), [
                    { field: "vec", value: vec2d },
                ]);

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // Search with a 3D query vector (12 bytes instead of 8)
                const vec3d = Buffer.alloc(12);

                await expect(
                    GlideFt.search(
                        client,
                        index,
                        "*=>[KNN 1 @VEC $query_vec]",
                        {
                            params: [{ key: "query_vec", value: vec3d }],
                        },
                    ),
                ).rejects.toThrow(RequestError);

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.SEARCH vector search with zero vector",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const prefix = "{" + getRandomKey() + "}:";
                const index = prefix + "idx";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            {
                                type: "VECTOR",
                                name: "vec",
                                alias: "VEC",
                                attributes: {
                                    algorithm: "FLAT",
                                    distanceMetric: "L2",
                                    dimensions: 2,
                                },
                            },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                // Insert two vectors: zero and non-zero
                const zeroVec = Buffer.alloc(8); // [0.0, 0.0]
                const nonZeroVec = Buffer.alloc(8);
                nonZeroVec.writeFloatLE(3.0, 0);
                nonZeroVec.writeFloatLE(4.0, 4);

                await client.hset(Buffer.from(prefix + "zero"), [
                    { field: "vec", value: zeroVec },
                ]);
                await client.hset(Buffer.from(prefix + "nonzero"), [
                    { field: "vec", value: nonZeroVec },
                ]);

                await new Promise((resolve) =>
                    setTimeout(resolve, DATA_PROCESSING_TIMEOUT),
                );

                // Search with zero vector should work and return both results
                const result: FtSearchReturnType = await GlideFt.search(
                    client,
                    index,
                    "*=>[KNN 2 @VEC $query_vec]",
                    {
                        params: [{ key: "query_vec", value: zeroVec }],
                    },
                );
                expect(result[0]).toEqual(2);

                // The zero vector should be nearest to itself (distance 0)
                expect(result[1][0].key).toContain("zero");

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT.INFO on valid index returns expected fields",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const index = getRandomKey();
                const prefix = "{" + getRandomKey() + "}:";

                expect(
                    await GlideFt.create(
                        client,
                        index,
                        [
                            { type: "TEXT", name: "title" },
                            { type: "NUMERIC", name: "price" },
                            { type: "TAG", name: "category" },
                        ],
                        { dataType: "HASH", prefixes: [prefix] },
                    ),
                ).toEqual("OK");

                const info = await GlideFt.info(client, index);

                // Verify essential fields in the info response
                expect(info["index_name"]).toEqual(index);
                expect(info["key_type"]).toEqual("HASH");
                expect(info["key_prefixes"]).toEqual([prefix]);
                expect(info["fields"]).toBeDefined();
                expect(
                    (info["fields"] as unknown[]).length,
                ).toBeGreaterThanOrEqual(3);

                // Verify num_docs field exists (may be 0)
                expect(info).toHaveProperty("num_docs");

                await GlideFt.dropindex(client, index);
            },
            TIMEOUT,
        );

        it(
            "FT._LIST returns all created indexes",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const idx1 = getRandomKey();
                const idx2 = getRandomKey();
                const idx3 = getRandomKey();

                expect(
                    await GlideFt.create(client, idx1, [
                        { type: "TEXT", name: "f1" },
                    ]),
                ).toEqual("OK");
                expect(
                    await GlideFt.create(client, idx2, [
                        { type: "TEXT", name: "f2" },
                    ]),
                ).toEqual("OK");
                expect(
                    await GlideFt.create(client, idx3, [
                        { type: "TEXT", name: "f3" },
                    ]),
                ).toEqual("OK");

                const listed = await GlideFt.list(client);
                expect(listed).toContain(idx1);
                expect(listed).toContain(idx2);
                expect(listed).toContain(idx3);

                // Cleanup
                await GlideFt.dropindex(client, idx1);
                await GlideFt.dropindex(client, idx2);
                await GlideFt.dropindex(client, idx3);
            },
            TIMEOUT,
        );
    });

    describe("GlideBf", () => {
        let client: GlideClusterClient;

        afterEach(async () => {
            await flushAndCloseClient(true, cluster?.getAddresses(), client);
        });

        it(
            "reserve + info",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                expect(
                    await GlideBf.reserve(client, key, 0.001, 10000),
                ).toEqual("OK");

                const info = await GlideBf.info(client, key);
                expect(info.capacity).toEqual(10000);
                expect(info.numberOfFilters).toBeGreaterThanOrEqual(1);
                expect(info.numberOfItems).toEqual(0);
                expect(info.size).toBeGreaterThan(0);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "add returns boolean",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // First add should return true (new item)
                expect(await GlideBf.add(client, key, "item1")).toBe(true);
                // Second add of same item should return false (already exists)
                expect(await GlideBf.add(client, key, "item1")).toBe(false);
                // Adding a different item should return true
                expect(await GlideBf.add(client, key, "item2")).toBe(true);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "madd",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Add single item first
                expect(await GlideBf.add(client, key, "item1")).toBe(true);

                // madd with mix of new and existing items
                const results = await GlideBf.madd(client, key, [
                    "item1",
                    "item2",
                    "item3",
                ]);
                expect(results).toEqual([false, true, true]);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "exists",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                await GlideBf.add(client, key, "item1");

                expect(await GlideBf.exists(client, key, "item1")).toBe(true);
                expect(await GlideBf.exists(client, key, "missing")).toBe(
                    false,
                );

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "mexists",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                await GlideBf.madd(client, key, ["item1", "item2"]);

                const results = await GlideBf.mexists(client, key, [
                    "item1",
                    "item2",
                    "missing",
                ]);
                expect(results).toEqual([true, true, false]);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "insert with default options",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Insert creates filter if it doesn't exist
                const results = await GlideBf.insert(client, key, [
                    "item1",
                    "item2",
                ]);
                expect(results).toEqual([true, true]);

                // Inserting existing items
                const results2 = await GlideBf.insert(client, key, [
                    "item1",
                    "item3",
                ]);
                expect(results2).toEqual([false, true]);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "insert with NOCREATE",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // NOCREATE on non-existent key should throw
                await expect(
                    GlideBf.insert(client, key, ["item1"], { noCreate: true }),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "insert with CAPACITY and ERROR",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                const results = await GlideBf.insert(
                    client,
                    key,
                    ["item1", "item2"],
                    {
                        capacity: 5000,
                        errorRate: 0.01,
                    },
                );
                expect(results).toEqual([true, true]);

                const info = await GlideBf.info(client, key);
                expect(info.capacity).toEqual(5000);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "card",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Non-existent key returns 0
                expect(await GlideBf.card(client, key)).toEqual(0);

                await GlideBf.madd(client, key, ["a", "b", "c"]);
                expect(await GlideBf.card(client, key)).toEqual(3);

                // Adding duplicate should not increase cardinality
                await GlideBf.add(client, key, "a");
                expect(await GlideBf.card(client, key)).toEqual(3);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with EXPANSION",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                expect(
                    await GlideBf.reserve(client, key, 0.01, 100, {
                        expansion: 4,
                    }),
                ).toEqual("OK");

                const info = await GlideBf.info(client, key);
                expect(info.capacity).toEqual(100);
                expect(info.expansionRate).toEqual(4);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with NONSCALING",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                expect(
                    await GlideBf.reserve(client, key, 0.01, 100, {
                        nonScaling: true,
                    }),
                ).toEqual("OK");

                const info = await GlideBf.info(client, key);
                expect(info.capacity).toEqual(100);
                // When nonScaling is set, expansion rate should be 0
                expect(info.expansionRate).toEqual(0);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve duplicate key error",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                expect(
                    await GlideBf.reserve(client, key, 0.01, 100),
                ).toEqual("OK");

                // Reserving on an existing key should throw
                await expect(
                    GlideBf.reserve(client, key, 0.01, 100),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "load with invalid data throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Loading invalid data should throw an error
                await expect(
                    GlideBf.load(client, key, Buffer.from("invalid_data")),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        // --- Edge case tests: reserve boundary values ---

        it(
            "reserve with errorRate 0 throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // errorRate 0 is outside the valid range (0, 1) exclusive
                await expect(
                    GlideBf.reserve(client, key, 0, 100),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with errorRate 1 throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // errorRate 1 is outside the valid range (0, 1) exclusive
                await expect(
                    GlideBf.reserve(client, key, 1, 100),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with very small errorRate (0.0001)",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Very small error rate should create a larger filter
                expect(
                    await GlideBf.reserve(client, key, 0.0001, 1000),
                ).toEqual("OK");

                const info = await GlideBf.info(client, key);
                expect(info.capacity).toEqual(1000);
                // A lower error rate requires more bits per item, so size should be larger
                expect(info.size).toBeGreaterThan(0);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with capacity 0 throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                await expect(
                    GlideBf.reserve(client, key, 0.01, 0),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "reserve with negative capacity throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                await expect(
                    GlideBf.reserve(client, key, 0.01, -10),
                ).rejects.toThrow(RequestError);

                await client.del([key]);
            },
            TIMEOUT,
        );

        // --- Edge case tests: add/exists with special values ---

        it(
            "add empty string as item",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Empty string is a valid item
                expect(await GlideBf.add(client, key, "")).toBe(true);
                expect(await GlideBf.exists(client, key, "")).toBe(true);

                // Adding same empty string again should return false
                expect(await GlideBf.add(client, key, "")).toBe(false);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "add very long string (10KB)",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                const longItem = "x".repeat(10240); // 10KB string

                expect(await GlideBf.add(client, key, longItem)).toBe(true);
                expect(await GlideBf.exists(client, key, longItem)).toBe(true);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "madd with duplicates in same call",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // When duplicates appear in the same madd call,
                // the first occurrence is new (true), subsequent are not (false)
                const results = await GlideBf.madd(client, key, [
                    "dup",
                    "dup",
                    "dup",
                ]);
                expect(results[0]).toBe(true);
                expect(results[1]).toBe(false);
                expect(results[2]).toBe(false);

                // Cardinality should be 1
                expect(await GlideBf.card(client, key)).toEqual(1);

                await client.del([key]);
            },
            TIMEOUT,
        );

        // --- Edge case tests: operations on non-existent keys ---

        it(
            "add to non-existent key auto-creates filter",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // BF.ADD auto-creates the filter if it does not exist
                expect(await GlideBf.add(client, key, "auto_item")).toBe(true);

                // Verify the filter was created via info
                const info = await GlideBf.info(client, key);
                expect(info.numberOfItems).toEqual(1);
                expect(info.capacity).toBeGreaterThan(0);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "exists on non-existent key returns false",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // BF.EXISTS on a non-existent key should return false
                expect(await GlideBf.exists(client, key, "anything")).toBe(
                    false,
                );
            },
            TIMEOUT,
        );

        it(
            "info on non-existent key throws",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                await expect(GlideBf.info(client, key)).rejects.toThrow(
                    RequestError,
                );
            },
            TIMEOUT,
        );

        it(
            "card on non-existent key returns 0",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                expect(await GlideBf.card(client, key)).toEqual(0);
            },
            TIMEOUT,
        );

        // --- Edge case tests: false positive rate verification ---

        it(
            "false positive rate within bounds",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();
                const errorRate = 0.05; // 5%
                const numItems = 1000;

                await GlideBf.reserve(client, key, errorRate, numItems);

                // Add 1000 items with prefix "in_"
                const addItems: string[] = [];

                for (let i = 0; i < numItems; i++) {
                    addItems.push(`in_${i}`);
                }

                // Add in batches
                for (let i = 0; i < addItems.length; i += 100) {
                    await GlideBf.madd(
                        client,
                        key,
                        addItems.slice(i, i + 100),
                    );
                }

                // Check 1000 items that were NOT added (prefix "out_")
                let falsePositives = 0;

                for (let i = 0; i < numItems; i++) {
                    const exists = await GlideBf.exists(
                        client,
                        key,
                        `out_${i}`,
                    );

                    if (exists) {
                        falsePositives++;
                    }
                }

                const observedRate = falsePositives / numItems;

                // Allow 2x the configured error rate as slack for statistical variance
                expect(observedRate).toBeLessThan(errorRate * 2);

                await client.del([key]);
            },
            TIMEOUT * 2,
        );

        // --- Edge case tests: insert edge cases ---

        it(
            "insert with noCreate on existing filter works",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // Create the filter first
                expect(
                    await GlideBf.reserve(client, key, 0.01, 100),
                ).toEqual("OK");

                // noCreate on an existing filter should succeed
                const results = await GlideBf.insert(
                    client,
                    key,
                    ["x", "y", "z"],
                    { noCreate: true },
                );
                expect(results).toEqual([true, true, true]);

                // Verify items exist
                const exists = await GlideBf.mexists(client, key, [
                    "x",
                    "y",
                    "z",
                ]);
                expect(exists).toEqual([true, true, true]);

                await client.del([key]);
            },
            TIMEOUT,
        );

        it(
            "insert creates filter if not exists",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                // insert without prior reserve should auto-create
                const results = await GlideBf.insert(client, key, [
                    "alpha",
                    "beta",
                ]);
                expect(results).toEqual([true, true]);

                // Verify filter was created
                const info = await GlideBf.info(client, key);
                expect(info.numberOfItems).toEqual(2);

                await client.del([key]);
            },
            TIMEOUT,
        );

        // --- Edge case tests: concurrent adds ---

        it(
            "concurrent adds from two clients both succeed",
            async () => {
                client = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const client2 = await GlideClusterClient.createClient(
                    getClientConfigurationOption(
                        cluster.getAddresses(),
                        ProtocolVersion.RESP3,
                    ),
                );

                const key = getRandomKey();

                try {
                    await GlideBf.reserve(client, key, 0.01, 1000);

                    // Concurrently add items from two clients
                    const [result1, result2] = await Promise.all([
                        GlideBf.add(client, key, "concurrent_item"),
                        GlideBf.add(client2, key, "concurrent_item"),
                    ]);

                    // One should be true (new) and one should be false (already existed),
                    // or both true if processed truly simultaneously (race).
                    // In either case, the item must exist after both complete.
                    expect(
                        await GlideBf.exists(client, key, "concurrent_item"),
                    ).toBe(true);

                    // At least one of the adds must have reported new
                    expect(result1 || result2).toBe(true);

                    await client.del([key]);
                } finally {
                    client2.close();
                }
            },
            TIMEOUT,
        );
    });
});
