// The node:net/node:tls transport, kept OUT of the shared core so the core module graph never imports
// them — runtimes that provide their own socket (Cloudflare via cloudflare:sockets, Deno, etc.) never
// pull node:tls, which doesn't exist on workerd even with nodejs_compat. The Node entry (minipg/node)
// registers this as the default transport via registerDefaultTransport(); other entries pass config.socket.
import net from 'node:net'
import tls from 'node:tls'
import type { Duplex } from 'node:stream'
import { W } from './protocol.ts'
import type { NormalizedConfig } from './connection.ts'

type TlsSsl = true | 'require' | 'verify-ca' | 'verify-full' | tls.ConnectionOptions
/** sslmode string (or tls.ConnectionOptions) → options for tls.connect (caller adds `socket`).
 *  'require'/true encrypt without verifying; 'verify-ca' verifies the chain but not the hostname;
 *  'verify-full' verifies both; an object verifies by default and is fully overridable. */
export function tlsOptions(ssl: TlsSsl, host: string): tls.ConnectionOptions {
  const isIP = !!net.isIP(host) // SNI must be a hostname, not an IP (RFC 6066)
  const servername = isIP ? undefined : host
  let o: tls.ConnectionOptions
  if (ssl === true || ssl === 'require') o = { servername, rejectUnauthorized: false }
  else if (ssl === 'verify-ca') o = { servername, rejectUnauthorized: true, checkServerIdentity: () => undefined }
  else if (ssl === 'verify-full') o = { servername, rejectUnauthorized: true }
  else o = { servername, rejectUnauthorized: true, ...ssl }
  // Connecting by IP with verification on and no override: verify the IP against the cert's IP SANs.
  if (o.rejectUnauthorized !== false && isIP && !o.servername && !o.checkServerIdentity)
    o.checkServerIdentity = (_s: string, cert: tls.PeerCertificate) => tls.checkServerIdentity(host, cert)
  return o
}

/** Open a TCP (or unix) socket, optionally negotiate PG SSLRequest → TLS, and resolve the byte duplex.
 *  On `signal` abort (connect deadline / failed attempt) it destroys the socket so a stalled handshake
 *  can't leak an open connection (the caller can't reach it — it isn't assigned until this resolves). */
export function nodeTransport(cfg: NormalizedConfig, signal: AbortSignal): Promise<Duplex> {
  return new Promise<Duplex>((resolve, reject) => {
    const sock = cfg.path ? net.connect({ path: cfg.path }) : net.connect({ host: cfg.host, port: cfg.port })
    let tlsSock: tls.TLSSocket | undefined
    signal.addEventListener('abort', () => { try { sock.destroy() } catch { /* */ } try { tlsSock?.destroy() } catch { /* */ } reject(new Error('connect aborted')) }, { once: true })
    sock.once('error', reject) // connect-phase failures reject; after resolve this is a harmless no-op
    sock.once('connect', () => {
      if (!cfg.ssl || cfg.path) return resolve(sock) // plaintext / unix: no SSL negotiation
      sock.write(W.sslRequest())
      sock.once('data', (buf: Buffer) => {
        const res = String.fromCharCode(buf[0]!)
        if (res === 'S') {
          tlsSock = tls.connect({ socket: sock, ...tlsOptions(cfg.ssl as TlsSsl, cfg.host) }, () => resolve(tlsSock!))
          tlsSock.once('error', reject)
        } else if (res === 'N') {
          reject(Object.assign(new Error('server does not support TLS'), { fatal: true })) // fail closed (no downgrade)
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
        const t = tls.connect({ socket: plain, ...tlsOptions(cfg.ssl as TlsSsl, cfg.host) }, () => send(t)); t.on('error', () => { /* */ })
      } else send(plain) // server declined SSL; try plaintext cancel
    })
  })
  plain.on('error', () => { /* best-effort */ })
}
