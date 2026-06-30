// Demo: the library's Shape API (whole-result-set codegen mapper), pretty-printed and
// syntax-highlighted for the terminal. No hand-rolled bytes / Function.
//   bun codegen-demo.ts        (or: bun run demo:codegen)
import { highlight } from 'cli-highlight'
import prettier from 'prettier'
import { JsonArray, Shape, connect } from './src/inline/index.ts'

const pretty = (src: string) => prettier.format(src, { parser: 'babel', semi: false, singleQuote: true, printWidth: 100 })
const colorize = (src: string) => highlight(src, { language: 'javascript', ignoreIllegals: true })
const show = async (src: string) => console.log(colorize(await pretty(src))) // prettier first (can't parse ANSI), then colorize

// 1) Declare a shape, inspect its generated result-set mapper — pure library API.
const user = Shape({
  id: 'int4', name: 'text', active: 'bool', balance: 'numeric', date: "date:temporal",
  big: 'int8:number', meta: 'jsonb', avatar: 'bytea',
  subscriptions: JsonArray({ id: "int4",  })
})
console.log('# Shape({...}).$mapper()  — object mode, maps ALL rows:\n')
await show(user.$mapper())

// 2) Run a matching query through the real driver (same codegen path, real wire bytes).
//    Needs the local cluster: `bun run test:setup`.
try {
  const conn = await connect({ host: '127.0.0.1', port: 54329, user: 'postgres', password: 'postgres', database: 'testdb' })
  const r = await conn.query(
    `select g::int4 as id, ('alice' || g)::text as name, (g % 2 = 0) as active, (g * 1.5)::numeric as balance,
            (9007199254740990 + g)::int8 as big, jsonb_build_object('g', g) as meta, '\\xdeadbeef'::bytea as avatar
     from generate_series(1, 3) g`,
    [], { mode: 'object' },
  )
  console.log('\n# Driver-decoded rows (same codegen, real wire bytes):')
  console.dir(r.rows, { depth: null })
  await conn.end()
} catch (e) {
  console.log('\n(live query skipped — run `bun run test:setup` first):', (e as Error).message)
}
process.exit(0)


function rows(arr) {
  const n = arr.length,
    res = new Array(n)
  for (let i = 0; i < n; i++) {
    const b = arr[i]
    let o = 2,
      l
    // "id" oid=23 (int<-bytes)
    l = b.readInt32BE(o)
    o += 4
    let v0 = null
    if (l !== -1) {
      {
        let p = o,
          s = false,
          x = 0
        const e = o + l
        if (b[p] === 45) {
          s = true
          p++
        }
        for (; p < e; p++) x = x * 10 + (b[p] - 48)
        v0 = s ? -x : x
      }
      o += l
    }
    // "name" oid=25 (string)
    l = b.readInt32BE(o)
    o += 4
    let v1 = null
    if (l !== -1) {
      v1 = b.toString('utf8', o, o + l)
      o += l
    }
    // "active" oid=16 (bool)
    l = b.readInt32BE(o)
    o += 4
    let v2 = null
    if (l !== -1) {
      v2 = b[o] === 116
      o += l
    }
    // "balance" oid=1700 (string)
    l = b.readInt32BE(o)
    o += 4
    let v3 = null
    if (l !== -1) {
      v3 = b.toString('utf8', o, o + l)
      o += l
    }
    // "big" oid=20 (string)
    l = b.readInt32BE(o)
    o += 4
    let v4 = null
    if (l !== -1) {
      v4 = b.toString('utf8', o, o + l)
      o += l
    }
    // "meta" oid=3802 (json)
    l = b.readInt32BE(o)
    o += 4
    let v5 = null
    if (l !== -1) {
      v5 = JSON.parse(b.toString('utf8', o, o + l))
      o += l
    }
    // "avatar" oid=17 (bytea)
    l = b.readInt32BE(o)
    o += 4
    let v6 = null
    if (l !== -1) {
      {
        const s = b.toString('utf8', o, o + l)
        v6 =
          s.charCodeAt(0) === 92 && s.charCodeAt(1) === 120
            ? Buffer.from(s.slice(2), 'hex')
            : Buffer.from(s, 'utf8')
      }
      o += l
    }
    res[i] = { id: v0, name: v1, active: v2, balance: v3, big: v4, meta: v5, avatar: v6 }
  }
  return res
}