// The node:net/node:tls transport, kept OUT of the shared core so the core module graph never imports
// them — runtimes that provide their own socket (Cloudflare via cloudflare:sockets, Deno, etc.) never
// pull node:tls, which doesn't exist on workerd even with nodejs_compat. The Node entry (minipg/node)
// registers this as the default transport via registerDefaultTransport(); other entries pass config.socket.
import net from 'node:net'
import tls from 'node:tls'
import type { Duplex } from 'node:stream'
import { W } from './protocol.ts'
import type { NormalizedConfig } from './connection.ts'

/** Open a TCP (or unix) socket, optionally negotiate PG SSLRequest → TLS, and resolve the byte duplex. */
export function nodeTransport(cfg: NormalizedConfig): Promise<Duplex> {
  return new Promise<Duplex>((resolve, reject) => {
    const sock = cfg.path ? net.connect({ path: cfg.path }) : net.connect({ host: cfg.host, port: cfg.port })
    sock.once('error', reject) // connect-phase failures reject; after resolve this is a harmless no-op
    sock.once('connect', () => {
      if (!cfg.ssl || cfg.path) return resolve(sock) // plaintext / unix: no SSL negotiation
      sock.write(W.sslRequest())
      sock.once('data', (buf: Buffer) => {
        const res = String.fromCharCode(buf[0]!)
        if (res === 'S') {
          const base = { socket: sock, servername: net.isIP(cfg.host) ? undefined : cfg.host }
          const opts = typeof cfg.ssl === 'object' ? { ...base, ...cfg.ssl } : { ...base, rejectUnauthorized: false }
          const tlsSock = tls.connect(opts, () => resolve(tlsSock))
          tlsSock.once('error', reject)
        } else if (res === 'N') {
          if (cfg.ssl === true || cfg.ssl === 'require') return reject(Object.assign(new Error('server does not support SSL'), { fatal: true }))
          resolve(sock)
        } else reject(Object.assign(new Error('unexpected SSL response byte: ' + res), { fatal: true }))
      })
    })
  })
}

/** Best-effort out-of-band CancelRequest on a fresh throwaway connection (mirrors SSL negotiation). */
export function nodeCancel(cfg: NormalizedConfig, key: { pid: number; secret: number }): void {
  const cancel = W.cancelRequest(key.pid, key.secret)
  const send = (s: net.Socket | tls.TLSSocket) => { try { s.write(cancel); s.end() } catch { /* */ } }
  const plain = net.connect({ host: cfg.host, port: cfg.port }, () => {
    if (!cfg.ssl) return send(plain)
    plain.write(W.sslRequest())
    plain.once('data', (b: Buffer) => {
      if (String.fromCharCode(b[0]!) === 'S') {
        const opts = typeof cfg.ssl === 'object' ? { socket: plain, ...cfg.ssl } : { socket: plain, rejectUnauthorized: false }
        const t = tls.connect(opts, () => send(t)); t.on('error', () => { /* */ })
      } else send(plain) // server declined SSL; try plaintext cancel
    })
  })
  plain.on('error', () => { /* best-effort */ })
}
