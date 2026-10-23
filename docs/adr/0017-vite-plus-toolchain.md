# Vite+ builds, formats and lints, and bun test keeps the main suites

minipg builds with `vp pack`, formats with `vp fmt` and lints with `vp lint`, all from Vite+. One dev dependency and one `vite.config.ts` replace tsdown, prettier, a Vitest 2 setup and their configs. The unit and integration suites stay on `bun test`, because they import `bun:test` and use Bun runtime APIs that Vitest cannot run. `vp test` runs only the Cloudflare suite. It runs inside workerd through `@cloudflare/vitest-plugin`, so the suite tests the current Workers runtime. The old pool pinned a workerd from 2024, and that runtime hid two bugs in the Cloudflare transport.

## Considered options

- Standalone tsdown and oxlint. These are the same tools, without the `vite` alias pair, but each needs its own config. Moving there later costs little, because each block of `vite.config.ts` is that tool's config.
- Vite+ installed outside the repository, to keep the Cloudflare suite on Vitest 2. This needs a second `package.json`, and the suite keeps the old runtime.

## Consequences

- `vite-plus`, the `vite` alias and `overrides.vite` must carry the same version. Without the override, Bun installs a second Vite under `vitest`, and the type check of `vite.config.ts` fails.
- The tool versions move together. A fix in tsdown or oxlint arrives with the next Vite+ release.
- `vp` needs Node `^22.18`, `^24.11` or `>=26` on development machines and in CI. The published library keeps its own `engines.node`.
- Type-aware lint runs on TypeScript 7, so the repository uses TypeScript 7 for `tsc` as well.
