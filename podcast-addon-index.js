const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const Redis = require('ioredis');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ─── Redis ────────────────────────────────────────────────────────────────
let redis = null;
if (process.env.REDIS_URL) {
  redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 3, enableReadyCheck: false });
  redis.on('connect', function () { console.log('[Redis] Connected'); });
  redis.on('error', function (e) { console.error('[Redis] Error: ' + e.message); });
}

async function redisSave(token, entry) {
  if (!redis) return;
  try {
    await redis.set('pod:token:' + token, JSON.stringify({
      createdAt: entry.createdAt, lastUsed: entry.lastUsed, reqCount: entry.reqCount
    }));
  } catch (e) { console.error('[Redis] Save failed: ' + e.message); }
}

async function redisLoad(token) {
  if (!redis) return null;
  try { var d = await redis.get('pod:token:' + token); return d ? JSON.parse(d) : null; }
  catch (e) { return null; }
}

// ─── Token store ──────────────────────────────────────────────────────────
const TOKEN_CACHE = new Map();
const IP_CREATES = new Map();
const MAX_TOKENS_PER_IP = 10;
const RATE_MAX = 60;
const RATE_WINDOW_MS = 60000;

function generateToken() { return crypto.randomBytes(14).toString('hex'); }

function getOrCreateIpBucket(ip) {
  var now = Date.now(), b = IP_CREATES.get(ip);
  if (!b || now > b.resetAt) { b = { count: 0, resetAt: now + 86400000 }; IP_CREATES.set(ip, b); }
  return b;
}

async function getTokenEntry(token) {
  if (TOKEN_CACHE.has(token)) return TOKEN_CACHE.get(token);
  var saved = await redisLoad(token);
  if (!saved) return null;
  var entry = { createdAt: saved.createdAt, lastUsed: saved.lastUsed, reqCount: saved.reqCount, rateWin: [] };
  TOKEN_CACHE.set(token, entry);
  return entry;
}

function checkRateLimit(entry) {
  var now = Date.now();
  entry.rateWin = (entry.rateWin || []).filter(function (t) { return now - t < RATE_WINDOW_MS; });
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now); entry.lastUsed = now; entry.reqCount = (entry.reqCount || 0) + 1;
  return true;
}

async function tokenMiddleware(req, res, next) {
  var entry = await getTokenEntry(req.params.token);
  if (!entry) return res.status(404).json({ error: 'Invalid token.' });
  if (!checkRateLimit(entry)) return res.status(429).json({ error: 'Rate limit exceeded.' });
  req.tokenEntry = entry;
  if (entry.reqCount % 20 === 0) redisSave(req.params.token, entry);
  next();
}

function getBaseUrl(req) { return (req.headers['x-forwarded-proto'] || req.protocol) + '://' + req.get('host'); }
function cleanText(s) { return String(s || '').replace(/\s+/g, ' ').trim(); }

// ─── Episode cache ────────────────────────────────────────────────────────
const EPISODE_CACHE = new Map();

function cacheEpisode(ep) {
  if (!ep || !ep.id) return;
  EPISODE_CACHE.set(String(ep.id), ep);
}

function durationSec(ms) {
  if (!ms) return null;
  var n = Number(ms);
  return isNaN(n) ? null : Math.floor(n / 1000);
}

// ─── iTunes helpers ───────────────────────────────────────────────────────
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

async function itunesGet(endpoint, params) {
  try {
    var r = await axios.get('https://itunes.apple.com' + endpoint, {
      params: params,
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      timeout: 12000
    });
    return r.data;
  } catch (e) { console.warn('[iTunes] ' + e.message); return null; }
}

function artworkHd(url) {
  if (!url) return null;
  return url.replace(/\/\d+x\d+(bb|cc)\./, '/600x600bb.');
}

function detectFormat(url) {
  if (!url) return 'mp3';
  var u = url.toLowerCase().split('?')[0];
  if (u.endsWith('.m4a') || u.includes('/m4a/')) return 'm4a';
  if (u.endsWith('.aac')) return 'aac';
  if (u.endsWith('.ogg') || u.endsWith('.opus')) return 'ogg';
  return 'mp3';
}

function mapEpisode(ep) {
  var id = 'ep_' + String(ep.trackId);
  var obj = {
    id: id,
    title: cleanText(ep.trackName),
    artist: cleanText(ep.collectionName || ep.artistName),
    album: cleanText(ep.collectionName),
    duration: durationSec(ep.trackTimeMillis),
    artworkURL: artworkHd(ep.artworkUrl600 || ep.artworkUrl160),
    streamURL: ep.episodeUrl || null,
    format: detectFormat(ep.episodeUrl || '')
  };
  cacheEpisode(obj);
  return obj;
}

function mapShow(show) {
  return {
    id: 'show_' + String(show.collectionId),
    title: cleanText(show.collectionName),
    artist: cleanText(show.artistName),
    artworkURL: artworkHd(show.artworkUrl600 || show.artworkUrl100),
    trackCount: show.trackCount || null,
    year: show.releaseDate ? String(show.releaseDate).slice(0, 4) : null
  };
}

// ─── RSS helpers ──────────────────────────────────────────────────────────
function rssTag(block, tag) {
  var m = block.match(new RegExp('<' + tag + '[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>\\s*<\\/' + tag + '>', 'i'));
  if (m) return m[1].trim();
  m = block.match(new RegExp('<' + tag + '[^>]*>([\\s\\S]*?)<\\/' + tag + '>', 'i'));
  if (m) return m[1].replace(/<[^>]*>/g, '').trim();
  return '';
}

function rssAttr(block, tag, attr) {
  var m = block.match(new RegExp('<' + tag + '[^>]+' + attr + '=["\']([^"\'\\s>]*)["\']', 'i'));
  return m ? m[1] : '';
}

function parseDuration(s) {
  if (!s) return null;
  var parts = String(s).trim().split(':').map(Number);
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  if (parts.length === 1 && !isNaN(parts[0])) return parts[0];
  return null;
}

async function fetchRss(feedUrl) {
  try {
    var r = await axios.get(feedUrl, {
      headers: { 'User-Agent': UA, 'Accept': 'application/rss+xml, application/xml, text/xml, */*' },
      timeout: 15000, responseType: 'text',
      validateStatus: function (s) { return s < 500; }
    });
    return r.data || null;
  } catch (e) { return null; }
}

function parseRssToShow(xml) {
  if (!xml) return null;
  var chanMatch = xml.match(/<channel[^>]*>([\s\S]*?)<\/channel>/i);
  var chan = chanMatch ? chanMatch[1] : xml;
  var chanNoItems = chan.replace(/<item[\s\S]*$/i, '');

  var showTitle = rssTag(chanNoItems, 'title');
  var showArtist = rssTag(chan, 'itunes:author') || rssTag(chan, 'managingEditor') || '';
  var showArt = rssAttr(chan, 'itunes:image', 'href') || '';
  var showDesc = rssTag(chanNoItems, 'description') || rssTag(chanNoItems, 'itunes:summary') || '';

  var items = [];
  var itemRx = /<item[^>]*>([\s\S]*?)<\/item>/gi;
  var m;
  while ((m = itemRx.exec(xml)) !== null) items.push(m[1]);

  var tracks = items.map(function (item, i) {
    var encUrl = rssAttr(item, 'enclosure', 'url');
    if (!encUrl) return null;
    var guid = rssTag(item, 'guid') || encUrl;
    var hash = crypto.createHash('md5').update(guid).digest('hex').slice(0, 12);
    var id = 'rss_' + hash;
    var epTitle = rssTag(item, 'title');
    var epArt = rssAttr(item, 'itunes:image', 'href') || showArt;
    var dur = parseDuration(rssTag(item, 'itunes:duration'));
    var obj = {
      id: id,
      title: cleanText(epTitle) || ('Episode ' + (i + 1)),
      artist: cleanText(showArtist) || cleanText(showTitle),
      album: cleanText(showTitle),
      duration: dur,
      artworkURL: epArt || null,
      streamURL: encUrl,
      format: detectFormat(encUrl)
    };
    cacheEpisode(obj);
    return obj;
  }).filter(Boolean);

  return {
    title: cleanText(showTitle),
    artist: cleanText(showArtist),
    artworkURL: showArt || null,
    description: cleanText(showDesc).slice(0, 300),
    tracks: tracks
  };
}

// ─── Config page ──────────────────────────────────────────────────────────
function buildConfigPage(baseUrl) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Eclipse – Podcast Addon</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0c0c0f;color:#e8e8e8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px}
.logo{margin-bottom:20px}
.card{background:#131316;border:1px solid #1f1f26;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.55);margin-bottom:20px}
h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}
h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}
p.sub{font-size:14px;color:#777;margin-bottom:20px;line-height:1.6}
.tip{background:#0d1520;border:1px solid #1a2d42;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#5a8abe;line-height:1.7}
.tip b{color:#7cb8de}
.pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}
.pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#0d1a2a;color:#4a9eff;border:1px solid #1a3a5e}
.pill.g{background:#0d1f0d;color:#6db86d;border-color:#2d422a}
.lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#555;margin-bottom:8px;margin-top:16px}
input{width:100%;background:#0c0c0f;border:1px solid #222;border-radius:10px;color:#e8e8e8;font-size:14px;padding:12px 14px;margin-bottom:6px;outline:none;transition:border-color .15s}
input:focus{border-color:#4a9eff}
input::placeholder{color:#333}
.hint{font-size:12px;color:#484848;margin-bottom:12px;line-height:1.7}
.hint a{color:#4a9eff;text-decoration:none}
.hint code{background:#1a1a1a;padding:1px 5px;border-radius:4px;color:#888}
button{cursor:pointer;border:none;border-radius:10px;font-size:15px;font-weight:700;padding:13px;width:100%;margin-top:6px;margin-bottom:12px;transition:background .15s}
.bo{background:#4a9eff;color:#fff}.bo:hover{background:#2a7fdf}.bo:disabled{background:#252525;color:#444;cursor:not-allowed}
.bg{background:#1a4a20;color:#e8e8e8;border:1px solid #2a6a30}.bg:hover{background:#245c2a}.bg:disabled{background:#252525;color:#444;cursor:not-allowed}
.bd{background:#1a1a1a;color:#aaa;border:1px solid #222;font-size:13px;padding:10px}.bd:hover{background:#222;color:#fff}
.box{display:none;background:#0c0c0f;border:1px solid #1e1e2e;border-radius:12px;padding:18px;margin-bottom:14px}
.blbl{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}
.burl{font-size:12px;color:#4a9eff;word-break:break-all;font-family:'SF Mono',monospace;margin-bottom:14px;line-height:1.5}
hr{border:none;border-top:1px solid #1a1a1a;margin:24px 0}
.steps{display:flex;flex-direction:column;gap:12px}
.step{display:flex;gap:12px;align-items:flex-start}
.sn{background:#1a1a1a;border:1px solid #252525;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#666}
.st{font-size:13px;color:#666;line-height:1.6}
.st b{color:#aaa}
.warn{background:#14100a;border:1px solid #2e2000;border-radius:10px;padding:14px;margin-top:20px;font-size:12px;color:#8a6a30;line-height:1.7}
.badge{display:inline-block;background:#0d1a2a;color:#4a9eff;border:1px solid #1a3a5e;border-radius:20px;font-size:11px;font-weight:600;padding:3px 10px;margin-bottom:14px}
.status{font-size:13px;color:#666;margin:8px 0;min-height:18px}
.status.ok{color:#5a9e5a}
.status.err{color:#c0392b}
.preview{background:#0c0c0f;border:1px solid #1a1a1a;border-radius:10px;padding:12px;max-height:200px;overflow-y:auto;margin-bottom:12px;display:none}
.tr{display:flex;gap:10px;align-items:center;padding:5px 0;border-bottom:1px solid #181818;font-size:13px}
.tr:last-child{border-bottom:none}
.tn{color:#444;font-size:11px;min-width:22px;text-align:right}
.ti{flex:1;min-width:0}
.tt{color:#e8e8e8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ta{color:#666;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
footer{margin-top:32px;font-size:12px;color:#333;text-align:center;line-height:1.8}
</style>
</head>
<body>

<svg class="logo" width="52" height="52" viewBox="0 0 52 52" fill="none">
  <circle cx="26" cy="26" r="26" fill="#1a3a5e"/>
  <rect x="20" y="10" width="12" height="22" rx="6" fill="#4a9eff"/>
  <path d="M14 28c0 6.627 5.373 12 12 12s12-5.373 12-12" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round" fill="none"/>
  <line x1="26" y1="40" x2="26" y2="45" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round"/>
  <line x1="20" y1="45" x2="32" y2="45" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round"/>
</svg>

<div class="card">
  <h1>Podcasts for Eclipse</h1>
  <div class="tip"><b>Save your URL</b> — copy it to Notes or a bookmark. If the server restarts, paste it below to restore access.</div>
  <p class="sub">Streams any podcast from Apple Podcasts or any RSS feed. Episodes, shows, and creators — all browsable in Eclipse.</p>
  <div class="pills">
    <span class="pill">Episodes &amp; shows</span>
    <span class="pill">Creator pages</span>
    <span class="pill g">RSS feed support</span>
    <span class="pill g">Offline download</span>
  </div>

  <div class="lbl">Generate Your Addon URL</div>
  <button class="bo" id="genBtn" onclick="generate()">Generate My Addon URL</button>
  <div class="box" id="genBox">
    <div class="blbl">Your addon URL — paste into Eclipse</div>
    <div class="burl" id="genUrl"></div>
    <button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button>
  </div>

  <hr>
  <div class="lbl">Refresh existing URL</div>
  <input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">
  <div class="hint">Same URL, same library — nothing breaks.</div>
  <button class="bg" id="refBtn" onclick="doRefresh()">Refresh Existing URL</button>
  <div class="box" id="refBox">
    <div class="blbl">Refreshed — same URL, still works in Eclipse</div>
    <div class="burl" id="refUrl"></div>
    <button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button>
  </div>

  <hr>
  <div class="steps">
    <div class="step"><div class="sn">1</div><div class="st">Generate and copy your URL above</div></div>
    <div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> → Settings → Connections → Add Connection → Addon</div></div>
    <div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap <b>Install</b></div></div>
    <div class="step"><div class="sn">4</div><div class="st">Use the <b>Podcast Importer</b> below to import any show into Eclipse Library</div></div>
  </div>
  <div class="warn">Your URL is saved to Redis and survives restarts. Never reinstall the addon after updates.</div>
</div>

<div class="card">
  <span class="badge">Podcast Importer</span>
  <h2>Import a Podcast to Eclipse Library</h2>
  <p class="sub">Fetches all episodes as a CSV you can import in Eclipse via Library → Import CSV.</p>
  <div class="lbl">Your Addon URL</div>
  <input type="text" id="impToken" placeholder="Paste your addon URL (auto-fills after generating)">
  <div class="lbl">Podcast URL</div>
  <input type="text" id="impUrl" placeholder="https://podcasts.apple.com/... or any RSS feed URL">
  <div class="hint">
    Apple Podcasts: <code>podcasts.apple.com/us/podcast/name/id123456789</code><br>
    RSS feed: any direct RSS/XML URL
  </div>
  <div class="status" id="impStatus"></div>
  <div class="preview" id="impPreview"></div>
  <button class="bg" id="impBtn" onclick="doImport()">Fetch &amp; Download CSV</button>
</div>

<footer>Eclipse Podcast Addon v1.0.0 &bull; <a href="${baseUrl}/health" target="_blank" style="color:#333;text-decoration:none">${baseUrl}</a></footer>

<script>
var gu, ru;

function generate() {
  var btn = document.getElementById('genBtn');
  btn.disabled = true;
  btn.textContent = 'Generating...';
  fetch('/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: '{}'
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.error) { alert(d.error); btn.disabled = false; btn.textContent = 'Generate My Addon URL'; return; }
    gu = d.manifestUrl;
    document.getElementById('genUrl').textContent = gu;
    document.getElementById('genBox').style.display = 'block';
    document.getElementById('impToken').value = gu;
    btn.disabled = false;
    btn.textContent = 'Regenerate URL';
  })
  .catch(function(e) { alert('Error: ' + e.message); btn.disabled = false; btn.textContent = 'Generate My Addon URL'; });
}

function copyGen() {
  if (!gu) return;
  navigator.clipboard.writeText(gu).then(function() {
    var b = document.getElementById('copyGenBtn');
    b.textContent = 'Copied!';
    setTimeout(function() { b.textContent = 'Copy URL'; }, 1500);
  });
}

function doRefresh() {
  var btn = document.getElementById('refBtn');
  var eu = document.getElementById('existingUrl').value.trim();
  if (!eu) { alert('Paste your existing addon URL first.'); return; }
  btn.disabled = true;
  btn.textContent = 'Refreshing...';
  fetch('/refresh', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ existingUrl: eu })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.error) { alert(d.error); btn.disabled = false; btn.textContent = 'Refresh Existing URL'; return; }
    ru = d.manifestUrl;
    document.getElementById('refUrl').textContent = ru;
    document.getElementById('refBox').style.display = 'block';
    document.getElementById('impToken').value = ru;
    btn.disabled = false;
    btn.textContent = 'Refresh Again';
  })
  .catch(function(e) { alert('Error: ' + e.message); btn.disabled = false; btn.textContent = 'Refresh Existing URL'; });
}

function copyRef() {
  if (!ru) return;
  navigator.clipboard.writeText(ru).then(function() {
    var b = document.getElementById('copyRefBtn');
    b.textContent = 'Copied!';
    setTimeout(function() { b.textContent = 'Copy URL'; }, 1500);
  });
}

function getTok(s) {
  var m = s.match(/\/u\/([a-f0-9]{28})\//);
  return m ? m[1] : null;
}

function hesc(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function doImport() {
  var btn = document.getElementById('impBtn');
  var raw = document.getElementById('impToken').value.trim();
  var purl = document.getElementById('impUrl').value.trim();
  var st = document.getElementById('impStatus');
  var pv = document.getElementById('impPreview');

  if (!raw) { st.className = 'status err'; st.textContent = 'Paste your addon URL first.'; return; }
  if (!purl) { st.className = 'status err'; st.textContent = 'Paste a podcast URL.'; return; }

  var tok = getTok(raw);
  if (!tok) { st.className = 'status err'; st.textContent = 'Could not find your token in the URL.'; return; }

  btn.disabled = true;
  btn.textContent = 'Fetching...';
  st.className = 'status';
  st.textContent = 'Fetching episodes...';
  pv.style.display = 'none';

  fetch('/u/' + tok + '/import?url=' + encodeURIComponent(purl))
  .then(function(r) {
    if (!r.ok) return r.json().then(function(e) { throw new Error(e.error || 'Server error ' + r.status); });
    return r.json();
  })
  .then(function(data) {
    var tracks = data.tracks;
    if (!tracks || !tracks.length) throw new Error('No episodes found.');

    var rows = tracks.slice(0, 50).map(function(t, i) {
      return '<div class="tr"><span class="tn">' + (i + 1) + '</span><div class="ti"><div class="tt">' + hesc(t.title) + '</div><div class="ta">' + hesc(t.artist) + '</div></div></div>';
    }).join('');
    if (tracks.length > 50) rows += '<div class="tr" style="text-align:center;color:#555">and ' + (tracks.length - 50) + ' more</div>';

    pv.innerHTML = rows;
    pv.style.display = 'block';
    st.className = 'status ok';
    st.textContent = 'Found ' + tracks.length + ' episodes in ' + data.title;

    var lines = ['Title,Artist,Album,Duration'];
    tracks.forEach(function(t) {
      function ce(s) {
        s = String(s || '');
        if (s.indexOf(',') !== -1 || s.indexOf('"') !== -1) s = '"' + s.replace(/"/g, '""') + '"';
        return s;
      }
      lines.push([ce(t.title), ce(t.artist), ce(t.album || data.title), ce(t.duration || '')].join(','));
    });

    var blob = new Blob([lines.join('\n')], { type: 'text/csv' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = (data.title || 'podcast').replace(/[^a-zA-Z0-9 \-]/g, '').trim() + '.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    btn.disabled = false;
    btn.textContent = 'Fetch & Download CSV';
  })
  .catch(function(e) {
    st.className = 'status err';
    st.textContent = e.message;
    btn.disabled = false;
    btn.textContent = 'Fetch & Download CSV';
  });
}
</script>
</body>
</html>`;
}

// ─── Routes ───────────────────────────────────────────────────────────────

app.get('/', function (req, res) {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(buildConfigPage(getBaseUrl(req)));
});

app.post('/generate', async function (req, res) {
  var ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();
  var bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens today from this IP.' });
  var token = generateToken();
  var entry = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [] };
  TOKEN_CACHE.set(token, entry);
  await redisSave(token, entry);
  bucket.count++;
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json' });
});

app.post('/refresh', async function (req, res) {
  var raw = req.body && req.body.existingUrl ? String(req.body.existingUrl).trim() : '';
  var token = raw, m = raw.match(/\/u\/([a-f0-9]{28})\//);
  if (m) token = m[1];
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste your full addon URL.' });
  var entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate a new one.' });
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

// ─── Manifest ─────────────────────────────────────────────────────────────
app.get('/u/:token/manifest.json', tokenMiddleware, function (req, res) {
  res.json({
    id: 'com.eclipse.podcasts.' + req.params.token.slice(0, 8),
    name: 'Podcasts',
    version: '1.0.0',
    description: 'Search and stream any podcast from Apple Podcasts. Episodes, shows, and creators.',
    icon: 'https://is1-ssl.mzstatic.com/image/thumb/Purple126/v4/32/dc/fc/32dcfc3a-d9aa-46b3-85fb-2bc8a9d3a574/AppIcon-0-0-1x_U007emarketing-0-10-0-85-220.png/512x512bb.jpg',
    resources: ['search', 'stream', 'catalog'],
    types: ['track', 'album', 'artist']
  });
});

// ─── Search ───────────────────────────────────────────────────────────────
app.get('/u/:token/search', tokenMiddleware, async function (req, res) {
  var q = cleanText(req.query.q);
  if (!q) return res.json({ tracks: [], albums: [], artists: [] });

  try {
    var results = await Promise.all([
      itunesGet('/search', { term: q, media: 'podcast', entity: 'podcastEpisode', limit: 20, explicit: 'Yes' }),
      itunesGet('/search', { term: q, media: 'podcast', entity: 'podcast', limit: 10, explicit: 'Yes' }),
      itunesGet('/search', { term: q, media: 'podcast', entity: 'podcastAuthor', limit: 5 }).catch(function () { return null; })
    ]);

    var tracks = ((results[0] && results[0].results) || [])
      .filter(function (ep) { return ep.kind === 'podcast-episode' && ep.episodeUrl; })
      .map(mapEpisode);

    var albums = ((results[1] && results[1].results) || [])
      .filter(function (s) { return s.kind === 'podcast' || s.wrapperType === 'collection'; })
      .map(mapShow);

    var artists = ((results[2] && results[2].results) || [])
      .filter(function (a) { return a.artistId; })
      .map(function (a) {
        return {
          id: 'artist_' + String(a.artistId),
          name: cleanText(a.artistName),
          artworkURL: null,
          genres: a.primaryGenreName ? [a.primaryGenreName] : []
        };
      });

    res.json({ tracks: tracks, albums: albums, artists: artists });
  } catch (e) {
    console.error('[search] ' + e.message);
    res.status(500).json({ error: 'Search failed', tracks: [], albums: [], artists: [] });
  }
});

// ─── Stream ───────────────────────────────────────────────────────────────
app.get('/u/:token/stream/:id', tokenMiddleware, async function (req, res) {
  var id = req.params.id;

  var cached = EPISODE_CACHE.get(id);
  if (cached && cached.streamURL) {
    return res.json({ url: cached.streamURL, format: cached.format || 'mp3' });
  }

  if (id.startsWith('ep_')) {
    var trackId = id.replace('ep_', '');
    var data = await itunesGet('/lookup', { id: trackId });
    if (data && data.results) {
      var ep = data.results.find(function (r) { return r.kind === 'podcast-episode' && r.episodeUrl; });
      if (ep) {
        var mapped = mapEpisode(ep);
        return res.json({ url: mapped.streamURL, format: mapped.format });
      }
    }
  }

  return res.status(404).json({ error: 'Stream not found for id: ' + id });
});

// ─── Album (= Podcast show) ───────────────────────────────────────────────
app.get('/u/:token/album/:id', tokenMiddleware, async function (req, res) {
  var rawId = req.params.id;
  var collectionId = rawId.replace('show_', '');

  try {
    var data = await itunesGet('/lookup', { id: collectionId, entity: 'podcastEpisode', limit: 200 });
    if (!data || !data.results || data.results.length === 0) {
      return res.status(404).json({ error: 'Show not found.' });
    }

    var show = data.results.find(function (r) { return r.wrapperType === 'collection'; }) || data.results[0];
    var episodes = data.results.filter(function (r) { return r.kind === 'podcast-episode' && r.episodeUrl; });
    var tracks = episodes.map(mapEpisode);

    res.json({
      id: rawId,
      title: cleanText(show.collectionName),
      artist: cleanText(show.artistName),
      artworkURL: artworkHd(show.artworkUrl600 || show.artworkUrl100),
      year: show.releaseDate ? String(show.releaseDate).slice(0, 4) : null,
      description: show.description || null,
      trackCount: tracks.length,
      tracks: tracks
    });
  } catch (e) {
    console.error('[album] ' + e.message);
    res.status(500).json({ error: 'Show fetch failed.' });
  }
});

// ─── Artist (= Podcast creator / network) ─────────────────────────────────
app.get('/u/:token/artist/:id', tokenMiddleware, async function (req, res) {
  var rawId = req.params.id;
  var artistId = rawId.replace('artist_', '');

  try {
    var data = await itunesGet('/lookup', { id: artistId, entity: 'podcast', limit: 25 });
    if (!data || !data.results || data.results.length === 0) {
      return res.status(404).json({ error: 'Artist not found.' });
    }

    var artist = data.results.find(function (r) { return r.wrapperType === 'artist'; });
    var podcasts = data.results.filter(function (r) { return r.wrapperType === 'collection' || r.kind === 'podcast'; });
    var name = artist ? cleanText(artist.artistName) : (podcasts[0] ? cleanText(podcasts[0].artistName) : 'Unknown');

    res.json({
      id: rawId,
      name: name,
      artworkURL: podcasts[0] ? artworkHd(podcasts[0].artworkUrl600) : null,
      bio: null,
      genres: (artist && artist.primaryGenreName) ? [artist.primaryGenreName] : [],
      topTracks: [],
      albums: podcasts.map(mapShow)
    });
  } catch (e) {
    console.error('[artist] ' + e.message);
    res.status(500).json({ error: 'Artist fetch failed.' });
  }
});

// ─── Playlist (RSS feed) ──────────────────────────────────────────────────
app.get('/u/:token/playlist/:id', tokenMiddleware, async function (req, res) {
  var rawId = req.params.id;
  if (!rawId.startsWith('rss_')) return res.status(404).json({ error: 'Playlist not found.' });

  var encoded = rawId.slice(4);
  var feedUrl;
  try { feedUrl = Buffer.from(encoded, 'base64url').toString('utf8'); }
  catch (e) { return res.status(400).json({ error: 'Invalid playlist ID.' }); }

  var xml = await fetchRss(feedUrl);
  if (!xml) return res.status(404).json({ error: 'Could not fetch RSS feed.' });
  var show = parseRssToShow(xml);
  if (!show) return res.status(500).json({ error: 'Could not parse RSS feed.' });

  res.json({
    id: rawId,
    title: show.title,
    description: show.description,
    artworkURL: show.artworkURL,
    creator: show.artist,
    tracks: show.tracks
  });
});

// ─── Import (Apple Podcasts URL or RSS feed → episode list) ───────────────
app.get('/u/:token/import', tokenMiddleware, async function (req, res) {
  var inputUrl = cleanText(req.query.url);
  if (!inputUrl) return res.status(400).json({ error: 'Pass ?url= with an Apple Podcasts or RSS feed URL.' });

  var appleMatch = inputUrl.match(/\/id(\d{6,12})/);
  if (appleMatch) {
    var podId = appleMatch[1];
    var data = await itunesGet('/lookup', { id: podId, entity: 'podcastEpisode', limit: 300 });
    if (!data || !data.results || data.results.length === 0) {
      return res.status(404).json({ error: 'Podcast not found on Apple Podcasts.' });
    }
    var show = data.results.find(function (r) { return r.wrapperType === 'collection'; }) || data.results[0];
    var episodes = data.results.filter(function (r) { return r.kind === 'podcast-episode' && r.episodeUrl; });
    return res.json({
      title: cleanText(show.collectionName || show.trackName || 'Podcast'),
      artworkURL: artworkHd(show.artworkUrl600),
      tracks: episodes.map(mapEpisode)
    });
  }

  if (/^https?:\/\//i.test(inputUrl)) {
    var xml = await fetchRss(inputUrl);
    if (!xml) return res.status(404).json({ error: 'Could not fetch RSS feed at that URL.' });
    var parsed = parseRssToShow(xml);
    if (!parsed) return res.status(500).json({ error: 'Could not parse the RSS feed.' });
    return res.json({ title: parsed.title, artworkURL: parsed.artworkURL, tracks: parsed.tracks });
  }

  return res.status(400).json({ error: 'Use an Apple Podcasts URL (podcasts.apple.com/.../idXXXX) or a direct RSS feed URL.' });
});

// ─── Health ───────────────────────────────────────────────────────────────
app.get('/health', function (req, res) {
  res.json({
    status: 'ok',
    version: '1.0.0',
    redisConnected: !!(redis && redis.status === 'ready'),
    activeTokens: TOKEN_CACHE.size,
    cachedEpisodes: EPISODE_CACHE.size,
    timestamp: new Date().toISOString()
  });
});

app.listen(PORT, function () {
  console.log('Eclipse Podcast Addon v1.0.0 on port ' + PORT);
});
