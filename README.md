# TICK-TOCK DEFUSE · Connector TikTok LIVE

Nghe live TikTok của streamer rồi đẩy bình luận / quà / tim / follow / vào
phòng sang game qua WebSocket. Game nối tới:

```
wss://<tên-app>.onrender.com/ws?key=<ACCESS_CODE>
```

## Deploy lên Render (miễn phí)

**Cách nhanh nhất — Blueprint:**
1. Đẩy thư mục này lên một repo GitHub.
2. Trên Render: **New → Blueprint** → chọn repo → Render tự tạo service
   theo `render.yaml` (gói Free, Node 20).
3. Trong phần **Environment** của service, đặt:
   - `TIKTOK_USERNAME` = tên TikTok của streamer (không cần @)
   - `ACCESS_CODE` = Render tự sinh sẵn (xem lại trong Environment)
4. Đợi deploy xong, mở `https://<tên-app>.onrender.com/` — phải thấy
   `{"service":"tick-tock-defuse-connector","tiktok":"live"|"offline",...}`.

**Cách tay:** New → Web Service → chọn repo → Build `npm install`,
Start `node server.js`, rồi đặt 2 biến môi trường như trên.

## Nối game vào connector

Mở trang Admin của game với link tự điền (khỏi dán tay):

```
https://tick-tock-defuse-nguyentrongtan.vercel.app/admin?ws=wss://<tên-app>.onrender.com/ws&key=<ACCESS_CODE>
```

Mở link này 1 lần: Admin tự lưu URL + mã, rồi tự nối TikTok ngay khi
màn hiển thị đang mở ở tab khác cùng trình duyệt.

## Chạy local / test không cần live thật

```bash
npm install
TIKTOK_USERNAME=ten_tiktok ACCESS_CODE=test123 TEST_MODE=1 node server.js
```

- `TEST_MODE=1`: không nối TikTok thật, tự phát event giả (chat A/B/C,
  quà, tim, vào phòng, follow) mỗi 4 giây — để test game đầu cuối.
- `GET /health` → `{"ok":true}` · `GET /` → trạng thái connector.
- WebSocket sai mã trả HTTP 401.

## Lưu ý gói Free của Render

- Service ngủ sau ~15 phút không ai dùng. Lần đầu nối sau khi ngủ sẽ
  mất ~30–60 giây để nó tỉnh dậy — cứ mở game/Admin trước buổi live
  vài phút là được. Trong lúc live có kết nối WebSocket mở thì nó
  không ngủ.
- Muốn luôn tỉnh: nâng lên gói Starter (~$7/tháng) trong Render.

## Event gửi sang game

Mỗi event là 1 dòng JSON (đúng định dạng adapter của game đã hiểu):

| type | nội dung |
| --- | --- |
| `chat` | `{nickname, uniqueId, comment}` |
| `gift` | `{nickname, uniqueId, giftName, diamondCount, repeatCount}` — chỉ gửi lần cuối của chuỗi quà, không cộng giờ 2 lần |
| `like` | `{nickname, uniqueId, likeCount, totalLikeCount}` |
| `join` | `{nickname, uniqueId}` — khán giả vào phòng |
| `follow` | `{nickname, uniqueId}` |
| `share` | `{nickname, uniqueId}` |
| `welcome` | trạng thái connector khi game vừa nối |
| `ping` | giữ kết nối, game tự bỏ qua |
