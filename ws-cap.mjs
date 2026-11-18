import WebSocket from 'ws'
const ws = new WebSocket('ws://localhost:10099/ws?key=test123')
const counts = {}
const samples = []
ws.on('message', (d) => {
  const m = JSON.parse(d.toString())
  counts[m.type] = (counts[m.type] || 0) + 1
  if (samples.length < 12) samples.push(d.toString())
})
ws.on('error', (e) => { console.error('WS ERR', e.message); process.exit(1) })
setTimeout(() => {
  console.log('COUNTS:', JSON.stringify(counts))
  console.log('SAMPLES:'); samples.forEach(s => console.log(' ', s))
  ws.close(); process.exit(0)
}, 75000)
