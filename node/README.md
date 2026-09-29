# @glidemq/speedkey

Valkey/Redis client with direct NAPI bindings based on [valkey-glide](https://github.com/valkey-io/valkey-glide) core. No IPC socket - Rust talks directly to Node.js via NAPI.

Interim package - will be replaced by the official valkey-glide Node.js client once it ships formal Search, JSON, and Bloom module support.

## Modules

- **Search** (GlideFt): 5 commands - CREATE, DROPINDEX, _LIST, INFO, SEARCH
- **JSON** (GlideJson): 22 commands - full JSON manipulation
- **Bloom Filter** (GlideBf): 9 commands - RESERVE, ADD, MADD, EXISTS, MEXISTS, INFO, INSERT, CARD, LOADCHUNK

See [github.com/avifenesh/speedkey](https://github.com/avifenesh/speedkey) for full documentation.

## License

Apache-2.0
