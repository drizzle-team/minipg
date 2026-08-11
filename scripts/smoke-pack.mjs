// Import-resolution smoke test for the packed @drizzle-team/minipg tarball.
// Installs the tarball into a scratch dir and dynamic-imports every subpath
// export from there, under node — Bun's resolver is more permissive and would
// mask exports-map bugs that break Node consumers.
//   node scripts/smoke-pack.mjs <path-to-tgz>
import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const tgz = process.argv[2]
if (!tgz) {
  console.error('usage: node scripts/smoke-pack.mjs <tgz>')
  process.exit(2)
}

const PKG = '@drizzle-team/minipg'

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'minipg-smoke-'))
execFileSync('npm', ['init', '-y'], { cwd: scratch, stdio: 'ignore' })
execFileSync('npm', ['i', path.resolve(tgz)], { cwd: scratch, stdio: 'ignore' })

// Derived from the installed tarball's own manifest, not hand-mirrored from package.json,
// so a new export added upstream is smoke-tested automatically.
const manifest = JSON.parse(fs.readFileSync(path.join(scratch, 'node_modules', PKG, 'package.json'), 'utf8'))
const subpaths = Object.keys(manifest.exports).map((k) => k.replace(/^\./, ''))

// src/cf.ts statically imports cloudflare:sockets, which Node cannot resolve.
const probe = `
const PKG = ${JSON.stringify(PKG)}
const subpaths = ${JSON.stringify(subpaths)}
const expectedFail = { '/cf': 'ERR_UNSUPPORTED_ESM_URL_SCHEME' }
let failed = false
for (const s of subpaths) {
  try {
    await import(\`\${PKG}\${s}\`)
    if (expectedFail[s]) { console.error(\`unexpectedly resolved: \${PKG}\${s}\`); failed = true }
  } catch (e) {
    const expected = expectedFail[s]
    if (!expected || e.code !== expected) { console.error(\`\${PKG}\${s}: \${e.code ?? e.name} \${e.message}\`); failed = true }
  }
}
process.exit(failed ? 1 : 0)
`
fs.writeFileSync(path.join(scratch, 'probe.mjs'), probe)

try {
  execFileSync('node', ['probe.mjs'], { cwd: scratch, stdio: 'inherit' })
  process.exit(0)
} catch {
  process.exit(1)
}
