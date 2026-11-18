// ─── TICK-TOCK DEFUSE · Connector TikTok LIVE (chạy trên Render) ───────────
// Nghe live TikTok của streamer rồi đẩy bình luận / quà / tim / follow / vào
// phòng sang game qua WebSocket. Game nối tới:  wss://<app>.onrender.com/ws?key=<ACCESS_CODE>
//
// Cách nối TikTok: gói ttl-live tự ký ẩn danh bằng tài khoản khách của TikTok
// (không cần đăng nhập, không cần chìa khóa Euler, không cần máy ký riêng).
// Gói này được mang kèm trong thư mục ttl-live/ và build lúc deploy.
//
// Biến môi trường:
//   TIKTOK_USERNAME  tên TikTok của streamer, không cần @ (vd: nguyentrongtan)
//   TTL_SESSION_COOKIE  cookie đăng nhập TikTok ("sessionid=...; ...") — bắt buộc
//                    vì TikTok từ chối khách ẩn danh từ máy chủ thuê; bỏ trống
//                    thì thử bằng khách (thường bị từ chối).
//   ACCESS_CODE      mật khẩu nối WebSocket — Render tự sinh khi deploy bằng Blueprint
//   PORT             Render tự cấp (mặc định 10000)
//   TEST_MODE=1      không nối TikTok thật — tự phát event giả mỗi vài giây để test game
//
// Chạy local:  TIKTOK_USERNAME=xxx ACCESS_CODE=test123 node server.js

import http from 'node:http'
import { WebSocketServer } from 'ws'
import { TikTokLive, classifySocialAction } from './ttl-live/dist/index.js'

const USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '').trim()
const SESSION_COOKIE = (process.env.TTL_SESSION_COOKIE || '').trim()
const ACCESS_CODE = (process.env.ACCESS_CODE || '').trim()
const PORT = Number(process.env.PORT || 10000)
const TEST_MODE = process.env.TEST_MODE === '1'
const RETRY_MS = 30_000

let tiktokState = TEST_MODE ? 'test' : 'connecting' // live | offline | connecting | test
let roomId = null
let clients = new Set()
let startedAt = Date.now()
let live = null
let retryTimer = null

function log(...args) {
  console.log(new Date().toISOString(), ...args)
}

function broadcast(obj) {
  const text = JSON.stringify(obj)
  for (const ws of clients) {
    if (ws.readyState === 1) {
      try { ws.send(text) } catch { /* bỏ qua client lỗi */ }
    }
  }
}

function who(user) {
  return {
    nickname: user?.nickname || user?.uniqueId || 'Khán giả',
    uniqueId: user?.uniqueId || '',
    userId: String(user?.userId || user?.uniqueId || ''),
  }
}

function scheduleRetry(reason) {
  if (retryTimer) return
  tiktokState = 'offline'
  log(`Nối lại TikTok sau 30 giây (${reason})`)
  retryTimer = setTimeout(() => {
    retryTimer = null
    connectTikTok()
  }, RETRY_MS)
}

// ── HTTP: trang trạng thái + health check cho Render ──────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ ok: true }))
    return
  }
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify({
    service: 'tick-tock-defuse-connector',
    username: USERNAME || null,
    tiktok: tiktokState,
    roomId,
    clients: clients.size,
    uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
    testMode: TEST_MODE,
  }))
})

// ── WebSocket /ws — chỉ ai có đúng ?key= mới được nối ────────────────────
const wss = new WebSocketServer({ noServer: true })

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost')
  if (url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n')
    socket.destroy()
    return
  }
  if (ACCESS_CODE && url.searchParams.get('key') !== ACCESS_CODE) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n')
    socket.destroy()
    log('Từ chối 1 kết nối WebSocket sai mã truy cập')
    return
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req)
  })
})

wss.on('connection', (ws) => {
  clients.add(ws)
  log(`Game đã nối vào (đang có ${clients.size} kết nối)`)
  ws.send(JSON.stringify({
    type: 'welcome',
    service: 'tick-tock-defuse-connector',
    username: USERNAME || null,
    tiktok: tiktokState,
    roomId,
  }))
  ws.on('close', () => {
    clients.delete(ws)
    log(`Game đã ngắt (còn ${clients.size} kết nối)`)
  })
  ws.on('error', () => { try { ws.close() } catch { /* ignore */ } })
})

// Giữ kết nối sống: ping mỗi 30s (game tự bỏ qua ping)
setInterval(() => broadcast({ type: 'ping', t: Date.now() }), 30_000)

// ── TikTok LIVE (ttl-live, ký ẩn danh, không cần đăng nhập) ──────────────
async function connectTikTok() {
  if (TEST_MODE) return
  if (!USERNAME) {
    tiktokState = 'offline'
    log('Chưa có TIKTOK_USERNAME — đợi cấu hình rồi khởi động lại service')
    return
  }
  if (live) {
    try { live.disconnect() } catch { /* ignore */ }
    live = null
  }
  tiktokState = 'connecting'
  log(`Đang nối TikTok LIVE của @${USERNAME} (${SESSION_COOKIE ? 'bằng tài khoản đã đăng nhập' : 'khách ẩn danh, tự ký'})…`)
  const conn = new TikTokLive(USERNAME, SESSION_COOKIE ? { sessionCookie: SESSION_COOKIE } : {})
  live = conn

  conn.on('chat', (data) => {
    broadcast({ type: 'chat', ...who(data.user), comment: data.comment || '' })
  })
  conn.on('gift', (data) => {
    // Quà tặng theo chuỗi TikTok gửi 2 lần: đang tặng + tặng xong.
    // Chỉ gửi lần cuối để game không cộng giờ 2 lần cho 1 món quà.
    if (data.streakable && !data.repeatEnd) return
    broadcast({
      type: 'gift',
      ...who(data.user),
      giftName: data.giftName || 'Quà tặng',
      giftId: data.giftId ?? null,
      diamondCount: Number(data.diamondCount ?? 1) || 1,
      repeatCount: Number(data.repeatCount || 1),
      repeatEnd: !!data.repeatEnd,
    })
  })
  conn.on('like', (data) => {
    broadcast({
      type: 'like',
      ...who(data.user),
      likeCount: Number(data.count || 1),
      totalLikeCount: Number(data.total || 0),
    })
  })
  conn.on('member', (data) => {
    broadcast({ type: 'join', ...who(data.user) })
  })
  conn.on('social', (data) => {
    const kind = classifySocialAction(data.action).kind
    if (kind === 'follow') broadcast({ type: 'follow', ...who(data.user) })
    else if (kind === 'share') broadcast({ type: 'share', ...who(data.user) })
  })
  conn.on('connected', (state) => {
    roomId = state.roomId || null
    tiktokState = 'live'
    log(`Đã vào phòng live của @${USERNAME} (roomId ${roomId}, ${state.giftCount} loại quà)`)
    broadcast({ type: 'welcome', service: 'tick-tock-defuse-connector', username: USERNAME, tiktok: 'live', roomId })
  })
  conn.on('reconnecting', ({ attempt, delayMs }) => {
    tiktokState = 'connecting'
    log(`TikTok mất kết nối — thử nối lại lần ${attempt} sau ${delayMs}ms`)
  })
  conn.on('disconnected', ({ code }) => {
    tiktokState = 'offline'
    roomId = null
    log(`TikTok ngắt kết nối (${code}) — gói tự nối lại, nếu hết lượt sẽ thử mới`)
  })
  conn.on('error', (err) => {
    log(`Lỗi TikTok: ${err.message}`)
    if (!conn.connected) {
      live = null
      try { conn.disconnect() } catch { /* ignore */ }
      scheduleRetry(err.message)
    }
  })

  try {
    const state = await conn.connect()
    roomId = state.roomId || null
    tiktokState = 'live'
    log(`Đã vào phòng live của @${USERNAME} (roomId ${roomId}, ${state.giftCount} loại quà)`)
    broadcast({ type: 'welcome', service: 'tick-tock-defuse-connector', username: USERNAME, tiktok: 'live', roomId })
  } catch (err) {
    tiktokState = 'offline'
    log(`Chưa nối được TikTok (@${USERNAME} có thể chưa live): ${err.message}`)
    live = null
    try { conn.disconnect() } catch { /* ignore */ }
    scheduleRetry(err.message)
  }
}

// ── TEST_MODE: tự phát event giả để test game mà không cần live thật ─────
const FAKE_USERS = [
  { nickname: 'Minh Anh', uniqueId: 'minhanh99' },
  { nickname: 'Tuấn Kiệt', uniqueId: 'tuankiet' },
  { nickname: 'Bé Bông', uniqueId: 'bebong' },
  { nickname: 'Quốc Bảo', uniqueId: 'quocbao' },
  { nickname: 'Hạnh Nguyễn', uniqueId: 'hanhnguyen' },
]
const FAKE_GIFTS = [
  { giftName: 'Hoa hồng', diamondCount: 1 },
  { giftName: 'Trái tim', diamondCount: 5 },
  { giftName: 'Kem', diamondCount: 9 },
  { giftName: 'Vương miện', diamondCount: 199 },
]
function fakeEvent(i) {
  const u = FAKE_USERS[i % FAKE_USERS.length]
  const kind = i % 5
  if (kind === 0) return { type: 'chat', ...who(u), comment: ['A', 'B', 'C', 'A', 'START'][i % 5] }
  if (kind === 1) {
    const g = FAKE_GIFTS[i % FAKE_GIFTS.length]
    return { type: 'gift', ...who(u), ...g, giftId: 1, repeatCount: 1, repeatEnd: true }
  }
  if (kind === 2) return { type: 'like', ...who(u), likeCount: 10, totalLikeCount: 100 + i * 10 }
  if (kind === 3) return { type: 'join', ...who(u) }
  return { type: 'follow', ...who(u) }
}
if (TEST_MODE) {
  log('TEST_MODE bật — phát event giả mỗi 4 giây, không nối TikTok thật')
  let i = 0
  setInterval(() => { broadcast(fakeEvent(i++)) }, 4000)
}

server.listen(PORT, () => {
  log(`Connector chạy ở cổng ${PORT} — TikTok: @${USERNAME || '(chưa cấu hình)'} — TEST_MODE=${TEST_MODE ? 'bật' : 'tắt'}`)
  if (!ACCESS_CODE) log('CẢNH BÁO: chưa đặt ACCESS_CODE — ai cũng nối được WebSocket!')
  connectTikTok()
})
