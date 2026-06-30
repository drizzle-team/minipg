// Authentication helpers: MD5 and SCRAM-SHA-256 (SASL). Pure node:crypto.
import { createHash, createHmac, pbkdf2Sync, randomBytes, timingSafeEqual } from 'node:crypto'

const md5hex = (buf: Buffer) => createHash('md5').update(buf).digest('hex')

/** 'md5' + md5( md5(password + user) + salt ) */
export function md5Password(user: string, password: string, salt: Buffer): string {
  const inner = md5hex(Buffer.concat([Buffer.from(password, 'utf8'), Buffer.from(user, 'utf8')]))
  return 'md5' + md5hex(Buffer.concat([Buffer.from(inner, 'utf8'), salt]))
}

// SASLprep (RFC 4013) — common-case NFKC normalization. Fixes the silent 28P01
// failures on non-ASCII passwords that postgres.js has (it skips this entirely).
const saslprep = (s: string) => s.normalize('NFKC')

const xor = (a: Buffer, b: Buffer) => { const o = Buffer.alloc(a.length); for (let i = 0; i < a.length; i++) o[i] = a[i]! ^ b[i]!; return o }
const hmac = (key: Buffer | string, str: string) => createHmac('sha256', key).update(str).digest()
const sha256 = (buf: Buffer) => createHash('sha256').update(buf).digest()

export interface Scram {
  mechanism: 'SCRAM-SHA-256'
  clientFirst: string
  continue(serverFirst: string): string
  final(serverFinal: string): void
}

/** Minimal SCRAM-SHA-256 client state machine (no channel binding). */
export function scram(password: string): Scram {
  const clientNonce = randomBytes(18).toString('base64')
  const clientFirstBare = `n=*,r=${clientNonce}`
  let serverSignature: Buffer | null = null
  return {
    mechanism: 'SCRAM-SHA-256',
    clientFirst: `n,,${clientFirstBare}`, // gs2 header 'n,,' = no channel binding
    continue(serverFirst) {
      const attrs: Record<string, string> = {}
      for (const p of serverFirst.split(',')) attrs[p[0]!] = p.slice(2)
      const nonce = attrs.r!, saltB64 = attrs.s!, iterStr = attrs.i!
      if (!nonce.startsWith(clientNonce)) throw new Error('SCRAM: server nonce mismatch')
      const iterations = parseInt(iterStr, 10)
      if (!(iterations > 0 && iterations <= 100000)) throw new Error(`SCRAM: bad iteration count ${iterStr}`)
      const salt = Buffer.from(saltB64, 'base64')
      const saltedPassword = pbkdf2Sync(saslprep(password), salt, iterations, 32, 'sha256')
      const clientKey = hmac(saltedPassword, 'Client Key')
      const storedKey = sha256(clientKey)
      const clientFinalNoProof = `c=biws,r=${nonce}` // biws = base64('n,,')
      const authMessage = `${clientFirstBare},${serverFirst},${clientFinalNoProof}`
      const clientProof = xor(clientKey, hmac(storedKey, authMessage))
      serverSignature = hmac(hmac(saltedPassword, 'Server Key'), authMessage)
      return `${clientFinalNoProof},p=${clientProof.toString('base64')}`
    },
    final(serverFinal) {
      const part = serverFinal.split(',').find((p) => p.startsWith('v='))
      if (!part || !serverSignature) throw new Error('SCRAM: missing server signature')
      const got = Buffer.from(part.slice(2), 'base64')
      if (got.length !== serverSignature.length || !timingSafeEqual(got, serverSignature))
        throw new Error('SCRAM: server signature verification failed')
    },
  }
}
