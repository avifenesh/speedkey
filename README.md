# speedkey

[![npm](https://img.shields.io/npm/v/@glidemq/speedkey)](https://www.npmjs.com/package/@glidemq/speedkey)
[![license](https://img.shields.io/npm/l/@glidemq/speedkey)](https://github.com/avifenesh/speedkey/blob/main/LICENSE)

Valkey/Redis client with direct NAPI bindings based on [valkey-glide](https://github.com/valkey-io/valkey-glide) core. No IPC socket - Rust talks directly to Node.js via NAPI.

This is the client layer for [glide-mq](https://github.com/avifenesh/glide-mq). It will be replaced by the official valkey-glide NAPI client once it ships upstream. Until then, speedkey provides the same typed API surface that glide-mq depends on.

> **Note:** When valkey-glide ships formal Node.js support for Search, JSON, and Bloom modules, speedkey will be deprecated in favor of the official client. The API surface is designed to align with valkey-glide to minimize migration effort.

## Install

```bash
npm install @glidemq/speedkey
```

Requires [valkey-bundle](https://hub.docker.com/r/valkey/valkey-bundle) for Search, JSON, and Bloom module support:
```bash
docker run -d -p 6379:6379 valkey/valkey-bundle
```

## Module Support

### Search (GlideFt) - 5 commands

| Command | Method | Description |
|---------|--------|-------------|
| `FT.CREATE` | `GlideFt.create()` | Create a search index with HASH or JSON schema |
| `FT.SEARCH` | `GlideFt.search()` | Search an index with filters and KNN vector queries |
| `FT.DROPINDEX` | `GlideFt.dropindex()` | Delete an index |
| `FT.INFO` | `GlideFt.info()` | Get index metadata |
| `FT._LIST` | `GlideFt.list()` | List all indexes |

**Search 1.1/1.2 options supported:**

FT.CREATE: `score`, `language`, `skipInitialScan`, `minStemSize`, `withOffsets`/`noOffsets`, `noStopWords`/`stopWords`, `punctuation`. Fields: `sortable`, `nostem`, `weight`, `withsuffixtrie`/`nosuffixtrie`.

FT.SEARCH: `nocontent`, `dialect`, `verbatim`, `inorder`, `slop`, `sortby`, `scorer`.

**Not available in valkey-search:** FT.AGGREGATE, FT.EXPLAIN, FT.EXPLAINCLI, FT.PROFILE, FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE, FT._ALIASLIST. These are Redis Search (redis-stack) only.

### JSON (GlideJson) - 22 commands

Full JSON document manipulation: `SET`, `GET`, `MGET`, `MSET`, `DEL`, `FORGET`, `TYPE`, `CLEAR`, `TOGGLE`, `STRLEN`, `STRAPPEND`, `ARRAPPEND`, `ARRINSERT`, `ARRLEN`, `ARRPOP`, `ARRTRIM`, `ARRINDEX`, `NUMINCRBY`, `NUMMULTBY`, `OBJLEN`, `OBJKEYS`, `RESP`, `DEBUG`.

### Bloom Filter (GlideBf) - 9 commands

| Command | Method | Description |
|---------|--------|-------------|
| `BF.RESERVE` | `GlideBf.reserve()` | Create a bloom filter with error rate and capacity |
| `BF.ADD` | `GlideBf.add()` | Add an item (returns true if new) |
| `BF.MADD` | `GlideBf.madd()` | Add multiple items |
| `BF.EXISTS` | `GlideBf.exists()` | Check if item may exist |
| `BF.MEXISTS` | `GlideBf.mexists()` | Check multiple items |
| `BF.INFO` | `GlideBf.info()` | Get filter metadata |
| `BF.INSERT` | `GlideBf.insert()` | Add items with auto-create options |
| `BF.CARD` | `GlideBf.card()` | Get item count |
| `BF.LOAD` | `GlideBf.load()` | Restore a serialized filter |

## Usage

```typescript
import {
  GlideClient,
  GlideClusterClient,
  GlideJson,
  GlideFt,
  GlideBf,
} from "@glidemq/speedkey";

// Standalone
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
});

// Cluster
const cluster = await GlideClusterClient.createClient({
  addresses: [{ host: "127.0.0.1", port: 7000 }],
});
```

### JSON

```typescript
await GlideJson.set(client, "user:1", "$", '{"name":"Alice","age":30}');
const user = await GlideJson.get(client, "user:1", { path: "$" });

// Batch set
await GlideJson.mset(client, [
  { key: "user:2", path: "$", value: '{"name":"Bob"}' },
  { key: "user:3", path: "$", value: '{"name":"Carol"}' },
]);
```

### Vector Search

```typescript
// Create index with vector field (required by valkey-search)
await GlideFt.create(client, "docs-idx", [
  { type: "TAG", name: "category" },
  { type: "NUMERIC", name: "score" },
  {
    type: "VECTOR", name: "embedding",
    attributes: {
      algorithm: "HNSW",
      dimensions: 384,
      distanceMetric: "COSINE",
      type: "FLOAT32",
    },
  },
], { dataType: "HASH", prefixes: ["doc:"] });

// Store document with embedding
const embedding = new Float32Array(384); // from your embedding model
await client.hset("doc:1", {
  category: "ml",
  score: "95",
  embedding: Buffer.from(embedding.buffer),
});

// KNN search with pre-filter
const [total, results] = await GlideFt.search(
  client, "docs-idx",
  "@category:{ml}=>[KNN 5 @embedding $VEC AS dist]",
  { params: [{ key: "VEC", value: Buffer.from(embedding.buffer) }] },
);
```

### Bloom Filter

```typescript
await GlideBf.reserve(client, "emails", 0.001, 100000);
await GlideBf.add(client, "emails", "alice@example.com");

const exists = await GlideBf.exists(client, "emails", "alice@example.com"); // true
const missing = await GlideBf.exists(client, "emails", "unknown@x.com");   // false

const info = await GlideBf.info(client, "emails");
// { capacity: 100000, size: ..., numberOfFilters: 1, numberOfItems: 1, expansionRate: 2 }
```

## Core API

speedkey exposes the full valkey-glide BaseClient API (200+ commands) for strings, hashes, lists, sets, sorted sets, streams, HyperLogLog, geo, pub/sub, scripting, and more. See the [valkey-glide documentation](https://github.com/valkey-io/valkey-glide) for the complete command reference.

## License

Apache-2.0
