// Geometric + extension type decoders for shapes: built-in `point`, pgvector `vector`, and
// PostGIS `geometry`/`geography` (EWKB). Extension types have DYNAMIC OIDs (created per
// database), so shapes reference them by NAME via sentinel OIDs — the decoder is chosen by the
// declaration, never by the runtime OID. All are TEXT-wire-format decoders.
import type { CodegenCol } from './decode.ts'

export const OID_POINT = 600
export const EXT_VECTOR = -1000   // pgvector `vector` — sentinel (real OID is per-database)
export const EXT_GEOMETRY = -1001 // PostGIS `geometry`/`geography` — sentinel
export const EXT_HALFVEC = -1002   // pgvector `halfvec` (same text format as vector)
export const EXT_SPARSEVEC = -1003 // pgvector `sparsevec`: '{i:v,i:v}/dim' (1-based indices)
export const EXT_BOX2D = -1004     // PostGIS box2d: 'BOX(xmin ymin,xmax ymax)'
export const EXT_BOX3D = -1005     // PostGIS box3d: 'BOX3D(xmin ymin zmin,xmax ymax zmax)'

// ---- point: text is "(x,y)" --------------------------------------------------------------
export const parsePoint = (s: string): { x: number; y: number } => {
  const c = s.indexOf(',')
  return { x: Number(s.slice(1, c)), y: Number(s.slice(c + 1, -1)) }
}
const parsePointTuple = (s: string): [number, number] => {
  const c = s.indexOf(',')
  return [Number(s.slice(1, c)), Number(s.slice(c + 1, -1))]
}

// ---- vector: text is "[1,2.5,3]" (valid JSON) ----------------------------------------------
export const parseVector = (s: string): number[] => JSON.parse(s) as number[]

// ---- sparsevec: text is '{1:1.5,3:2}/5' (1-based indices / dimension) -----------------------
export interface SparseVec { dim: number; indices: number[]; values: number[] }
export function parseSparsevec(s: string): SparseVec {
  const slash = s.lastIndexOf('/')
  const dim = Number(s.slice(slash + 1))
  const body = s.slice(1, slash - 1) // strip '{' and '}'
  const indices: number[] = [], values: number[] = []
  if (body.length) for (const pair of body.split(',')) {
    const c = pair.indexOf(':')
    indices.push(Number(pair.slice(0, c)))
    values.push(Number(pair.slice(c + 1)))
  }
  return { dim, indices, values }
}
export function sparseToDense(v: SparseVec): number[] {
  const out = new Array<number>(v.dim).fill(0)
  for (let i = 0; i < v.indices.length; i++) out[v.indices[i]! - 1] = v.values[i]! // 1-based
  return out
}

// ---- box2d/box3d: 'BOX(xmin ymin,xmax ymax)' / 'BOX3D(xmin ymin zmin,xmax ymax zmax)' -------
export function parseBox(s: string): number[] {
  const open = s.indexOf('(')
  const [lo, hi] = s.slice(open + 1, -1).split(',')
  return [...lo!.trim().split(/\s+/), ...hi!.trim().split(/\s+/)].map(Number)
}

// ---- geometry/geography: text is hex-encoded (E)WKB ----------------------------------------
export interface GeoJson { type: string; coordinates?: unknown; geometries?: GeoJson[]; srid?: number }
const GEO_TYPES = ['', 'Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'] as const

/** Parse (E)WKB bytes -> GeoJSON-shaped object. Handles both EWKB dimension flags (PostGIS)
 *  and ISO WKB type offsets (1000=Z, 2000=M, 3000=ZM), nested byte orders, and SRID. */
export function parseWkb(b: Buffer, pos = 0): { geo: GeoJson; end: number } {
  const le = b[pos] === 1
  const u32 = (o: number): number => (le ? b.readUInt32LE(o) : b.readUInt32BE(o))
  const f64 = (o: number): number => (le ? b.readDoubleLE(o) : b.readDoubleBE(o))
  const raw = u32(pos + 1)
  let p = pos + 5
  const hasZflag = (raw & 0x80000000) !== 0, hasMflag = (raw & 0x40000000) !== 0
  const hasSrid = (raw & 0x20000000) !== 0
  const isoBase = (raw & 0x0fffffff) % 1000
  const isoDims = Math.floor((raw & 0x0fffffff) / 1000) // 0, 1(Z), 2(M), 3(ZM)
  const typeId = isoBase
  const dims = 2 + (hasZflag || isoDims === 1 || isoDims === 3 ? 1 : 0) + (hasMflag || isoDims === 2 || isoDims === 3 ? 1 : 0)
  let srid: number | undefined
  if (hasSrid) { srid = u32(p); p += 4 }
  const point = (): number[] => { const c: number[] = []; for (let d = 0; d < dims; d++) { c.push(f64(p)); p += 8 } return c }
  const ring = (): number[][] => { const n = u32(p); p += 4; const out: number[][] = []; for (let i = 0; i < n; i++) out.push(point()); return out }
  const type = GEO_TYPES[typeId]
  if (!type) throw new Error(`minipg: unsupported WKB geometry type ${typeId}`)
  let geo: GeoJson
  switch (type) {
    case 'Point': geo = { type, coordinates: point() }; break
    case 'LineString': geo = { type, coordinates: ring() }; break
    case 'Polygon': { const n = u32(p); p += 4; const rings: number[][][] = []; for (let i = 0; i < n; i++) rings.push(ring()); geo = { type, coordinates: rings }; break }
    default: { // Multi* / GeometryCollection: n nested FULL geometries (each with its own byte-order header)
      const n = u32(p); p += 4
      const parts: GeoJson[] = []
      for (let i = 0; i < n; i++) { const r = parseWkb(b, p); parts.push(r.geo); p = r.end }
      geo = type === 'GeometryCollection'
        ? { type, geometries: parts }
        : { type, coordinates: parts.map((g) => g.coordinates) }
    }
  }
  if (srid !== undefined) geo.srid = srid
  return { geo, end: p }
}
export const parseEwkbHex = (hex: string): GeoJson => parseWkb(Buffer.from(hex, 'hex')).geo

// ---- shape-column dispatcher (shared by BOTH decode engines) --------------------------------
type AtDecoder = (b: Buffer, o: number, l: number) => unknown

/** Decoder for a geometric/extension shape column. Claims ONLY explicit parse targets — bare
 *  declarations ('point', 'vector', 'geometry') default to the raw TEXT string via the normal
 *  string machinery (returning null here). Parsing is opt-in per column. */
export function extAt(col: Pick<CodegenCol, 'oid' | 'js'>): AtDecoder | null {
  const js = col.js as string | undefined
  switch (col.oid) {
    case OID_POINT:
      if (js === 'xy') return (b, o, l) => parsePoint(b.toString('utf8', o, o + l))
      if (js === 'tuple') return (b, o, l) => parsePointTuple(b.toString('utf8', o, o + l))
      return null // bare / :string -> raw '(x,y)' text
    case EXT_VECTOR:
      if (js === 'array') return (b, o, l) => parseVector(b.toString('utf8', o, o + l))
      if (js === 'f32') return (b, o, l) => Float32Array.from(JSON.parse(b.toString('utf8', o, o + l)) as number[])
      return null // bare / :string -> raw '[…]' text
    case EXT_GEOMETRY:
      if (js === 'geojson') return (b, o, l) => parseEwkbHex(b.toString('utf8', o, o + l))
      if (js === 'wkb') return (b, o, l) => Buffer.from(b.toString('utf8', o, o + l), 'hex')
      return null // bare / :hex / :string -> raw EWKB hex text
    case EXT_HALFVEC: // same '[…]' text as vector
      if (js === 'array') return (b, o, l) => parseVector(b.toString('utf8', o, o + l))
      if (js === 'f32') return (b, o, l) => Float32Array.from(JSON.parse(b.toString('utf8', o, o + l)) as number[])
      return null
    case EXT_SPARSEVEC:
      if (js === 'sparse') return (b, o, l) => parseSparsevec(b.toString('utf8', o, o + l))
      if (js === 'array') return (b, o, l) => sparseToDense(parseSparsevec(b.toString('utf8', o, o + l)))
      return null // bare / :string -> raw '{i:v,…}/dim' text
    case EXT_BOX2D: case EXT_BOX3D: {
      if (js === 'tuple') return (b, o, l) => parseBox(b.toString('utf8', o, o + l))
      if (js === 'xy') {
        const is3d = col.oid === EXT_BOX3D
        return (b, o, l) => {
          const n = parseBox(b.toString('utf8', o, o + l))
          return is3d
            ? { xmin: n[0], ymin: n[1], zmin: n[2], xmax: n[3], ymax: n[4], zmax: n[5] }
            : { xmin: n[0], ymin: n[1], xmax: n[2], ymax: n[3] }
        }
      }
      return null // bare / :string -> raw 'BOX(…)' text
    }
    default: return null
  }
}
