// Runs every TypeScript example in the README's Effect section against the local cluster.
import { test, expect } from 'bun:test'
import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../../', import.meta.url))
const readme = await Bun.file(`${root}README.md`).text()
const section = readme.slice(readme.indexOf('\n## Effect\n'))
const examples = [...section.matchAll(/```ts\n([\s\S]*?)```/g)].map((m) => m[1]!)

const tmp = `${root}test/.readme-examples`

test('the README Effect section has examples', () => {
  expect(examples.length).toBeGreaterThanOrEqual(4)
})

for (const [i, code] of examples.entries()) {
  test(`README Effect example ${i + 1} runs`, async () => {
    mkdirSync(tmp, { recursive: true })
    const file = `${tmp}/example-${i}.ts`
    writeFileSync(file, code
      .replaceAll("'@drizzle-team/minipg/effect/core'", `'${root}src/effect/core.ts'`)
      .replaceAll("'@drizzle-team/minipg/effect'", `'${root}src/effect/index.ts'`)
      .replaceAll("'@drizzle-team/minipg'", `'${root}src/index.ts'`))
    try {
      const proc = Bun.spawn(['bun', file], { stdout: 'pipe', stderr: 'pipe' })
      const [code, stderr] = await Promise.all([proc.exited, new Response(proc.stderr).text()])
      expect(stderr).toBe('')
      expect(code).toBe(0)
    } finally { rmSync(file, { force: true }) }
  }, 15000)
}
