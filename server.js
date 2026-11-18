// ─── TICK-TOCK DEFUSE · Connector TikTok LIVE (chạy trên Render) ───────────
// Nghe live TikTok của streamer rồi đẩy bình luận / quà / tim / follow / vào
// phòng sang game qua WebSocket. Game nối tới:  wss://<app>.onrender.com/ws?key=<ACCESS_CODE>
//
// Cách nối TikTok (MANAGED WebSocket qua Euler, gói Community miễn phí):
//   1. Lấy roomId TRỰC TIẾP từ TikTok (không qua Euler): kiểm tra user có đang
//      live không rồi lấy roomId bằng các route HTML/API của tiktok-live-connector.
//      Đường dự phòng qua Euler trong các route này đã TẮT hẳn.
//   2. Gọi Euler rooms.fetchWebcastURL(roomId) bằng tiktok-live-api-sdk —
//      Euler trả về gói protobuf ProtoMessageFetchResult (đường nối WebSocket
//      TikTok + con trỏ/cursor + message ban đầu). Đường này dùng được với key
//      Community miễn phí (đã kiểm chứng: trả 200). Đường KÝ trực tiếp
//      (fetchWebcastSignature) thì KHÔNG dùng được với key miễn phí nên bỏ hẳn.
//   3. Mở WebSocket tới đường nối đó bằng WebSocket client của chính
//      tiktok-live-connector (tự lo nhịp tim, ack, giải mã protobuf). IP máy
//      mình chỉ chạm vào TikTok ở bước 1 (đọc trang live, giống mở web thường);
//      đường WebSocket do Euler cấp.
// Không cần cookie đăng nhập TikTok.
//
// Biến môi trường:
//   TIKTOK_USERNAME  tên TikTok của streamer, không cần @ (vd: ineed3_)
//   EULER_API_KEY    chìa khóa API Euler Stream (lấy ở eulerstream.com) — bắt buộc
//   ACCESS_CODE      mật khẩu nối WebSocket — Render tự sinh khi deploy bằng Blueprint
//   PORT             Render tự cấp (mặc định 10000)
//   TEST_MODE=1      không nối TikTok thật — tự phát event giả mỗi vài giây để test game
//
// Chạy local:  TIKTOK_USERNAME=xxx EULER_API_KEY=xxx ACCESS_CODE=test123 node server.js

import http from 'node:http'
import { WebSocketServer } from 'ws'
import TikTokLiveApiSdk from 'tiktok-live-api-sdk'
import {
  WebcastHttpClient, getWebConfig, getRandomPresets,
  getWebSocketConfigDefaults, createWebSocketProvider,
  fetchRoomIdComposite, fetchIsLiveComposite,
  RoomIdRouteConfig, IsLiveRouteConfig,
  deserializeMessage, ControlAction, LIBRARY_IDENTITY,
} from 'tiktok-live-connector'

// TẮT hẳn đường dự phòng qua Euler khi lấy roomId / kiểm tra live:
// chỉ dùng HTML + API trực tiếp của TikTok.
RoomIdRouteConfig.skipFetchRoomIdFromEulerRoute = true
IsLiveRouteConfig.skipFetchRoomIdFromEulerRoute = true

const USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '').trim()
const EULER_API_KEY = (process.env.EULER_API_KEY || '').trim()
const ACCESS_CODE = (process.env.ACCESS_CODE || '').trim()
const PORT = Number(process.env.PORT || 10000)
const TEST_MODE = process.env.TEST_MODE === '1'
const RETRY_MS = 30_000

let tiktokState = TEST_MODE ? 'test' : 'connecting' // live | offline | connecting | test
let roomId = null
let clients = new Set()
let startedAt = Date.now()
let live = null // WebcastWebSocketClient đang nối TikTok (hoặc null)
let retryTimer = null
let decodeFailCount = 0

// Client TikTok trực tiếp (bước 1) + client WebSocket (bước 3), dựng 1 lần.
const presets = getRandomPresets()
const webClient = new WebcastHttpClient(getWebConfig(presets))
const wsProvider = createWebSocketProvider(getWebSocketConfigDefaults(presets))
const euler = new TikTokLiveApiSdk({ apiKey: EULER_API_KEY })

function log(...args) {
  console.log(new Date().toISOString(), ...args)
}

// ── Bộ đệm event để game bắt kịp sau khi rớt mạng ─────────────────────────
// iPhone tự tắt tab game ở dưới nền → WebSocket rớt, event gửi đúng lúc đó
// sẽ mất. Connector giữ lại event 5 phút gần nhất (tối đa 200); game nối
// lại sẽ gửi {type:'sync', lastSeq} và nhận lại các event còn thiếu.
// Mỗi event có số thứ tự tăng dần (seq) để game bỏ qua event trùng.
const CATCHUP_KEEP_MS = 5 * 60 * 1000
const CATCHUP_MAX = 200
const CATCHUP_TYPES = new Set(['chat', 'gift', 'like', 'join', 'follow', 'share'])
let eventSeq = 0
const recentEvents = []

function broadcast(obj) {
  if (obj && CATCHUP_TYPES.has(obj.type)) {
    eventSeq += 1
    obj.seq = eventSeq
    obj.t = Date.now()
    recentEvents.push(obj)
    const cutoff = Date.now() - CATCHUP_KEEP_MS
    while (recentEvents.length && recentEvents[0].t < cutoff) recentEvents.shift()
    if (recentEvents.length > CATCHUP_MAX) recentEvents.splice(0, recentEvents.length - CATCHUP_MAX)
  }
  const text = JSON.stringify(obj)
  for (const ws of clients) {
    if (ws.readyState === 1) {
      try { ws.send(text) } catch { /* bỏ qua client lỗi */ }
    }
  }
}

// Lấy thông tin người xem từ event TikTok.
// Event đã giải mã của tiktok-live-connector: user lồng trong data.user,
// tên hiển thị ở displayId, id ở idStr (đọc thêm cả tên cũ uniqueId/userId để
// chắc chắn nếu thư viện đổi tên field trong bản sau).
function who(data) {
  const user = data?.user ?? data ?? {}
  const uniqueId = user?.displayId || user?.uniqueId || ''
  return {
    nickname: user?.nickname || uniqueId || 'Khán giả',
    uniqueId,
    userId: String(user?.idStr ?? user?.userId ?? user?.id ?? uniqueId),
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
  // Game nối lại sau khi rớt mạng (vd: tab Safari ở dưới nền bị iPhone tắt):
  // nó gửi {type:'sync', lastSeq} — trả lại các event nó chưa nhận, tối đa 200.
  ws.on('message', (data) => {
    let msg
    try { msg = JSON.parse(String(data)) } catch { return }
    if (!msg || msg.type !== 'sync') return
    const lastSeq = Number(msg.lastSeq) || 0
    const missed = recentEvents.filter((e) => e.seq > lastSeq).slice(-CATCHUP_MAX)
    try { ws.send(JSON.stringify({ type: 'catchup', events: missed })) } catch { /* bỏ qua */ }
  })
  ws.on('close', () => {
    clients.delete(ws)
    log(`Game đã ngắt (còn ${clients.size} kết nối)`)
  })
  ws.on('error', () => { try { ws.close() } catch { /* ignore */ } })
})

// Giữ kết nối sống: ping mỗi 30s (game tự bỏ qua ping)
setInterval(() => broadcast({ type: 'ping', t: Date.now() }), 30_000)

// ── TikTok LIVE (Euler MANAGED WebSocket, key Community miễn phí) ──────────

// Đẩy 1 event TikTok đã giải mã sang game (đúng shape broadcast cũ).
function handleDecoded({ type, data }) {
  switch (type) {
    case 'WebcastChatMessage':
      broadcast({ type: 'chat', ...who(data), comment: data.content ?? data.comment ?? '' })
      break
    case 'WebcastGiftMessage': {
      // Quà tặng theo chuỗi TikTok gửi 2 lần: đang tặng + tặng xong.
      // Chỉ gửi lần cuối để game không cộng giờ 2 lần cho 1 món quà.
      // Theo tài liệu thư viện: chỉ quà giftType 1 mới có chuỗi, nên bỏ qua
      // event giữa chuỗi (repeatEnd chưa xong) của đúng loại quà đó.
      const giftType = data.gift?.type ?? data.giftType
      if (giftType === 1 && !data.repeatEnd) break
      broadcast({
        type: 'gift',
        ...who(data),
        giftName: data.gift?.name || data.giftName || 'Quà tặng',
        giftId: data.giftId ?? null,
        diamondCount: Number(data.gift?.diamondCount ?? data.diamondCount ?? 1) || 1,
        repeatCount: Number(data.repeatCount || 1),
        repeatEnd: !!data.repeatEnd,
      })
      break
    }
    case 'WebcastLikeMessage':
      broadcast({
        type: 'like',
        ...who(data),
        likeCount: Number(data.count ?? data.likeCount ?? 1),
        totalLikeCount: Number(data.total ?? data.totalLikeCount ?? 0),
      })
      break
    case 'WebcastMemberMessage':
      broadcast({ type: 'join', ...who(data) })
      break
    case 'WebcastSocialMessage': {
      // Thư viện cũ tự tách social thành follow / share (dựa theo displayText
      // của TikTok) — làm lại đúng luật đó ở đây.
      const key = data.common?.displayText?.key || ''
      if (key.includes('follow')) broadcast({ type: 'follow', ...who(data) })
      else if (key.includes('share')) broadcast({ type: 'share', ...who(data) })
      break
    }
    case 'WebcastControlMessage':
      // Live kết thúc hoặc bị tạm dừng: đóng nối để thử lại sau 30 giây.
      if (data.action === ControlAction.CONTROL_ACTION_STREAM_ENDED ||
          data.action === ControlAction.CONTROL_ACTION_STREAM_SUSPENDED) {
        log('Phòng live đã kết thúc — thử lại sau 30 giây')
        try { live?.close() } catch { /* close sẽ tự thử lại */ }
      }
      break
    default:
      // Các loại event khác (roomUser, barrage, system...) game không dùng.
      break
  }
}

// Đẩy các message trong 1 gói ProtoMessageFetchResult sang game.
function handleFetchResult(result) {
  for (const message of result?.messages || []) {
    if (!message.decodedData) continue
    try {
      handleDecoded(message.decodedData)
    } catch (err) {
      log(`Bỏ qua 1 event TikTok lỗi: ${err?.message || err}`)
    }
  }
}

// Mở WebSocket tới đường nối Euler đã cấp (client của thư viện tự lo
// nhịp tim, ack và giải mã protobuf).
function openManagedWs(pushServer, wsParams, cookieHeader) {
  const wsClient = wsProvider({
    roomId,
    baseUrl: pushServer,
    wsParams,
    wsHeaders: { Cookie: cookieHeader || '' },
  })
  live = wsClient

  wsClient.on('open', () => {
    tiktokState = 'live'
    log(`Đã vào phòng live của @${USERNAME} (roomId ${roomId})`)
    broadcast({ type: 'welcome', service: 'tick-tock-defuse-connector', username: USERNAME, tiktok: 'live', roomId })
    try { wsClient.switchRooms(roomId) } catch { /* nối lại sẽ thử tiếp */ }
  })
  wsClient.on('protoMessageFetchResult', (result) => handleFetchResult(result))
  wsClient.on('imEnteredRoom', () => {
    log(`Đã vào phòng live của @${USERNAME} (roomId ${roomId})`)
  })
  wsClient.on('messageDecodingFailed', (err) => {
    decodeFailCount++
    if (decodeFailCount <= 3) log(`Không đọc được 1 gói tin TikTok (${decodeFailCount}): ${err?.message || err}`)
  })
  wsClient.on('unexpected-response', (_req, res) => {
    log(`TikTok từ chối nối WebSocket (mã ${res?.statusCode || '?'}) — thử lại sau 30 giây`)
    try { wsClient.close() } catch { /* close sẽ tự thử lại */ }
  })
  wsClient.on('close', (code) => {
    if (live !== wsClient) return
    live = null
    tiktokState = 'offline'
    roomId = null
    log(`Đường nối TikTok đóng (${code}) — thử lại sau 30 giây`)
    scheduleRetry(`đường nối đóng (${code})`)
  })
  wsClient.on('error', (err) => {
    log(`Lỗi đường nối TikTok: ${err?.message || err}`)
    // 'close' sẽ chạy theo sau và tự thử lại, nên ở đây chỉ ghi log.
  })
}

async function connectTikTok() {
  if (TEST_MODE) return
  if (!USERNAME) {
    tiktokState = 'offline'
    log('Chưa có TIKTOK_USERNAME — đợi cấu hình rồi khởi động lại service')
    return
  }
  if (!EULER_API_KEY) {
    tiktokState = 'offline'
    log('Chưa có EULER_API_KEY — chưa nối được TikTok, sẽ thử lại sau 30 giây')
    scheduleRetry('thiếu EULER_API_KEY')
    return
  }
  if (live) {
    try { live.close() } catch { /* ignore */ }
    live = null
  }
  tiktokState = 'connecting'
  try {
    // Bước 1: user có đang live không + roomId — TRỰC TIẾP từ TikTok.
    log(`Đang kiểm tra @${USERNAME} có đang live không…`)
    const isLive = await fetchIsLiveComposite({ uniqueId: USERNAME, webClient, apiClient: webClient.apiClient })
    if (!isLive) {
      tiktokState = 'offline'
      roomId = null
      log(`@${USERNAME} chưa live — thử lại sau 30 giây`)
      scheduleRetry('chưa live')
      return
    }
    roomId = await fetchRoomIdComposite({ uniqueId: USERNAME, webClient, apiClient: webClient.apiClient })
    log(`@${USERNAME} đang live (roomId ${roomId}) — đang lấy đường nối qua Euler…`)

    // Bước 2: Euler cấp đường nối WebSocket + con trỏ + message ban đầu.
    const res = await euler.rooms.fetchWebcastURL(
      String(roomId), LIBRARY_IDENTITY, undefined,
      webClient.clientHeaders?.['User-Agent'], true, 'web',
      undefined, undefined, { responseType: 'arraybuffer' },
    )
    if (res.status !== 200) throw new Error(`Euler trả mã ${res.status}`)
    const fetchResult = deserializeMessage('ProtoMessageFetchResult', Buffer.from(res.data))
    if (!fetchResult.pushServer || !fetchResult.cursor) {
      throw new Error('Euler không trả đủ thông tin nối phòng')
    }
    if (res.headers?.['x-set-tt-cookie']) {
      try { await webClient.cookieJar.processSetCookieHeader(res.headers['x-set-tt-cookie']) } catch { /* nối không cookie vẫn thử */ }
    }
    if (res.headers?.['x-room-id']) roomId = res.headers['x-room-id']
    handleFetchResult(fetchResult)

    // Bước 3: mở WebSocket (client của thư viện tự lo nhịp tim/ack/giải mã).
    let cookieHeader = ''
    try { cookieHeader = await webClient.cookieJar.getAnonymousCookieString() } catch { /* nối không cookie vẫn thử */ }
    const wsParams = {
      compress: 'gzip',
      room_id: roomId,
      internal_ext: fetchResult.internalExt,
      cursor: fetchResult.cursor,
      ...(fetchResult.routeParams || {}),
    }
    openManagedWs(fetchResult.pushServer, wsParams, cookieHeader)
  } catch (err) {
    tiktokState = 'offline'
    roomId = null
    log(`Chưa nối được TikTok: ${err?.message || err}`)
    live = null
    scheduleRetry(err?.message || 'không nối được')
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
  log(`Connector chạy ở cổng ${PORT} — TikTok: @${USERNAME || '(chưa cấu hình)'} — Euler: ${EULER_API_KEY ? 'đã có key' : 'CHƯA CÓ KEY'} — TEST_MODE=${TEST_MODE ? 'bật' : 'tắt'}`)
  if (!ACCESS_CODE) log('CẢNH BÁO: chưa đặt ACCESS_CODE — ai cũng nối được WebSocket!')
  connectTikTok()
})
