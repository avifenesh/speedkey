# speedkey

[![npm](https://img.shields.io/npm/v/@glidemq/speedkey)](https://www.npmjs.com/package/@glidemq/speedkey)
[![license](https://img.shields.io/npm/l/@glidemq/speedkey)](https://github.com/avifenesh/speedkey/blob/main/LICENSE)

Valkey/Redis client with direct NAPI bindings based on [valkey-glide](https://github.com/valkey-io/valkey-glide) core. No IPC socket - Rust talks directly to Node.js via NAPI.

This is the client layer for [glide-mq](https://github.com/avifenesh/glide-mq). It will be replaced by the official valkey-glide NAPI client once it ships upstream. Until then, speedkey provides the same typed API surface that glide-mq depends on.

Based on [valkey-glide PR #5325](https://github.com/valkey-io/valkey-glide/pull/5325).

## Install

```bash
npm install @glidemq/speedkey
```

## License

Apache-2.0
