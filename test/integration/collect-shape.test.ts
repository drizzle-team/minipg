// Collect() nested grouping + Transform() per-column decode-time fn + Nullable()/auto-null, run through BOTH
// mappers (decode:'jit' and decode:'interpreted') and asserted equal + against the expected nested rows.
// Uses VALUES-with-casts (no tables) to produce columns in a fixed wire order incl. NULLs (LEFT-JOIN miss).
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { testConnect, caught } from '../helpers/db.ts'
import { Collect, Transform, Nullable, Json } from '../../src/index.ts'
import type { Connection } from '../../src/index.ts'
import type { ShapeSpec } from '../../src/spec.ts'

let jit: Connection, interp: Connection
beforeAll(async () => { jit = await testConnect({ decode: 'jit' }); interp = await testConnect({ decode: 'interpreted' }) })
afterAll(async () => { await jit?.end(); await interp?.end() })

const eq = (a: unknown, b: unknown): boolean => {
  if (a === b) return true
  if (typeof a === 'bigint' || typeof b === 'bigint') return a === b
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime()
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false
  if (Array.isArray(a) || Array.isArray(b)) return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => eq(x, b[i]))
  const ka = Object.keys(a as object), kb = Object.keys(b as object)
  return ka.length === kb.length && ka.every((k) => eq((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]))
}

async function rows(sql: string, shape: ShapeSpec): Promise<unknown[]> {
  const j = (await jit.query(sql, [], { shape: shape as never })).rows
  const i = (await interp.query(sql, [], { shape: shape as never })).rows
  expect(eq(j, i)).toBe(true) // dual-variant: jit === interpreted
  return j
}

describe('Collect / Transform / Nullable', () => {
  test('nested grouping + auto-null when a required field is NULL', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,'alice'::text,10::int8,'pro'::text),(2::int8,'bob'::text,NULL::int8,NULL::text)) t(uid,uname,sid,splan)`
    const r = await rows(sql, { user: Collect({ id: 'bigint:number', name: 'text' }), sub: Collect({ id: 'bigint', plan: 'text' }) })
    expect(r[0]).toEqual({ user: { id: 1, name: 'alice' }, sub: { id: 10n, plan: 'pro' } })
    expect(r[1]).toEqual({ user: { id: 2, name: 'bob' }, sub: null }) // sub.id required + NULL -> whole group null
  })

  test('Transform is SKIPPED for NULL cells — null passes through (changed 2026-07-09)', async () => {
    const sql = `SELECT * FROM (VALUES (5::int8,'hi'::text),(9::int8,NULL::text)) t(id,note)`
    const r = await rows(sql, { id: Transform('bigint:number', (n: number) => n * 100), note: Transform('text', (s: string) => s.toUpperCase()) })
    expect(r[0]).toEqual({ id: 500, note: 'HI' })
    expect(r[1]).toEqual({ id: 900, note: null }) // fn never called; no null-check needed inside fn
  })

  test('Nullable field does not trigger the group auto-null', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,NULL::text)) t(id,bio)`
    const r = await rows(sql, { u: Collect({ id: 'bigint', bio: Nullable('text') }) })
    expect(r[0]).toEqual({ u: { id: 1n, bio: null } }) // bio Nullable -> group present, bio null
  })

  test('all-Nullable group with EVERY field null -> group is null (changed 2026-07-09)', async () => {
    const sql = `SELECT * FROM (VALUES (NULL::int8,NULL::text)) t(a,b)`
    const r = await rows(sql, { g: Collect({ a: Nullable('bigint'), b: Nullable('text') }) })
    expect(r[0]).toEqual({ g: null }) // was { a: null, b: null }; all-null now means LEFT-JOIN miss
  })

  test('Nullable(Transform(...)) composes; null skips the fn', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,NULL::text),(2::int8,'go'::text)) t(id,tag)`
    const r = await rows(sql, { u: Collect({ id: 'bigint:number', tag: Nullable(Transform('text', (s: string) => s.toUpperCase())) }) })
    expect(r[0]).toEqual({ u: { id: 1, tag: null } }) // fn not called on null
    expect(r[1]).toEqual({ u: { id: 2, tag: 'GO' } })
  })

  test('deep nesting (Collect in Collect) with inner auto-null', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,'nyc'::text,2::int8),(3::int8,NULL::text,NULL::int8)) t(id,city,zip)`
    const r = await rows(sql, { user: Collect({ id: 'bigint:number', address: Collect({ city: 'text', zip: 'bigint:number' }) }) })
    expect(r[0]).toEqual({ user: { id: 1, address: { city: 'nyc', zip: 2 } } })
    expect(r[1]).toEqual({ user: { id: 3, address: null } }) // address.city required + NULL -> address null (user present)
  })

  test('multiple required fields: any NULL nulls the group', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,NULL::int8,'x'::text)) t(a,b,c)`
    const r = await rows(sql, { g: Collect({ a: 'bigint', b: 'bigint', c: 'text' }) })
    expect(r[0]).toEqual({ g: null }) // b required + NULL
  })

  test('Json() inside a Collect (mixed marker nesting)', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8,'{"theme":"dark","n":3}'::json)) t(id,prefs)`
    const r = await rows(sql, { u: Collect({ id: 'bigint:number', prefs: Json({ theme: 'text', n: 'int4' }) }) })
    expect(r[0]).toEqual({ u: { id: 1, prefs: { theme: 'dark', n: 3 } } })
  })

  test('all-Nullable Collect group: ALL fields null -> group is null (LEFT-JOIN miss)', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8, NULL::text, NULL::int4), (2::int8, 'bob', NULL::int4)) t(id, name, age)`
    const r = await rows(sql, { id: 'bigint:number', u: Collect({ name: Nullable('text'), age: Nullable('int4') }) })
    expect(r[0]).toEqual({ id: 1, u: null })                      // every field null -> whole group null
    expect(r[1]).toEqual({ id: 2, u: { name: 'bob', age: null } }) // partial null -> object survives
  })

  test('all-Nullable NESTED group nulls independently of its parent', async () => {
    const sql = `SELECT * FROM (VALUES (1::int8, 'x', NULL::text, NULL::text)) t(id, a, b, c)`
    const r = await rows(sql, { id: 'bigint:number', g: Collect({ a: Nullable('text'), inner: Collect({ b: Nullable('text'), c: Nullable('text') }) }) })
    expect(r[0]).toEqual({ id: 1, g: { a: 'x', inner: null } }) // parent has data; empty child group nulls
  })

  test('Collect in array mode is rejected', async () => {
    const err = await caught(() => jit.query(`SELECT 1::int4 v`, [], { shape: { g: Collect({ v: 'int4' }) } as never, mode: 'array' }))
    expect((err as Error).message).toMatch(/Collect|object mode|nest/i)
  })

  test('two Transforms with the same type but different fns get different mappers (cache key)', async () => {
    const sql = `SELECT 5::int8 v`
    const a = (await jit.query(sql, [], { shape: { v: Transform('bigint:number', (n: number) => n + 1) } as never })).rows[0]
    const b = (await jit.query(sql, [], { shape: { v: Transform('bigint:number', (n: number) => n + 2) } as never })).rows[0]
    expect(a).toEqual({ v: 6 })
    expect(b).toEqual({ v: 7 }) // must NOT reuse a's compiled mapper
  })

  test('__proto__ group/leaf keys are own properties (no prototype pollution)', async () => {
    const sql = `SELECT * FROM (VALUES (7::int4)) t(v)`
    // computed keys create OWN '__proto__' props (a plain `{ __proto__: … }` literal is the prototype-setter)
    const r = await rows(sql, { ['__proto__']: Collect({ ['__proto__']: 'int4' } as never) } as ShapeSpec)
    const row = r[0] as Record<string, unknown>
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype)
    const grp = Object.getOwnPropertyDescriptor(row, '__proto__')!.value as Record<string, unknown>
    expect(Object.getOwnPropertyDescriptor(grp, '__proto__')!.value).toBe(7)
  })
})
