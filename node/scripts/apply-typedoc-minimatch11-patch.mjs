// typedoc 0.28.x's glob walker (dist/node-utils.js, glob().shouldRecurse) calls
// Minimatch#matchOne(childPath, row, /* partial */ true), a public method that
// was removed in minimatch 11. With the security override that forces typedoc's
// nested minimatch to 11.0.1 (GHSA-g3hv-75wg-m249, alert #46), every typedoc
// invocation throws "mini.matchOne is not a function".
//
// npm does not support `patchedDependencies` (silently ignored through at least
// npm 11.x) and postinstall hooks are unreliable under npm's install-scripts
// gating, so this script applies a one-line compatibility patch idempotently
// right before docs are built.
//
// The patch only affects directory-recurse pruning, which is an optimization:
// discoverFiles() still filters every file through controller.matches(), which
// uses the public Minimatch#match(). Returning true unconditionally recurses
// into more directories but produces identical output.
//
// No-op conditions (safe on future typedoc upgrades):
//   - already patched -> exit 0
//   - target snippet absent (typedoc no longer calls matchOne) -> exit 0
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));

let typedocEntry;
try {
    typedocEntry = require.resolve("typedoc");
} catch {
    console.error("patch-typedoc: typedoc is not installed; nothing to patch.");
    process.exit(0);
}
const target = join(dirname(dirname(typedocEntry)), "dist", "node-utils.js");

const BROKEN = `      return mini.set.some((row) => mini.matchOne(
        childPath,
        row,
        /* partial */
        true
      ));`;
const FIXED_MARKER = "minimatch >= 11 removed the public matchOne";
const FIXED = `      // ${FIXED_MARKER}: recurse permissively instead of partial
      // matching via matchOne(); controller.matches() still filters files.
      return true;`;

let src;
try {
    src = readFileSync(target, "utf8");
} catch (e) {
    console.error(`patch-typedoc: cannot read ${target}: ${e.message}`);
    process.exit(1);
}

if (src.includes(FIXED_MARKER)) {
    console.log("patch-typedoc: already patched; nothing to do.");
    process.exit(0);
}
if (!src.includes(BROKEN)) {
    console.log(
        "patch-typedoc: matchOne call site not found (typedoc no longer uses it); nothing to do."
    );
    process.exit(0);
}

writeFileSync(target, src.replace(BROKEN, FIXED));
console.log(`patch-typedoc: patched ${target} for minimatch 11 compatibility.`);
