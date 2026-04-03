const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const Redis   = require('ioredis');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ─── Redis ────────────────────────────────────────────────────────────────
let redis = null;
if (process.env.REDIS_URL) {
  redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 3, enableReadyCheck: false });
  redis.on('connect', () => console.log('[Redis] Connected'));
  redis.on('error',   e  => console.error('[Redis]', e.message));
}

async function redisSave(token, entry) {
  if (!redis) return;
  try {
    await redis.set('pod:token:' + token, JSON.stringify({
      createdAt: entry.createdAt, lastUsed: entry.lastUsed, reqCount: entry.reqCount,
      piKey: entry.piKey, piSecret: entry.piSecret, taddyKey: entry.taddyKey || ''
    }));
  } catch (e) { console.error('[Redis] Save failed:', e.message); }
}

async function redisLoad(token) {
  if (!redis) return null;
  try { const d = await redis.get('pod:token:' + token); return d ? JSON.parse(d) : null; }
  catch (e) { return null; }
}

// ─── Token store ──────────────────────────────────────────────────────────
const TOKEN_CACHE        = new Map();
const IP_CREATES         = new Map();
const MAX_TOKENS_PER_IP  = 10;
const RATE_MAX           = 60;
const RATE_WINDOW_MS     = 60000;

function generateToken() { return crypto.randomBytes(14).toString('hex'); }

function getOrCreateIpBucket(ip) {
  const now = Date.now();
  let b = IP_CREATES.get(ip);
  if (!b || now > b.resetAt) { b = { count: 0, resetAt: now + 86400000 }; IP_CREATES.set(ip, b); }
  return b;
}

async function getTokenEntry(token) {
  if (TOKEN_CACHE.has(token)) return TOKEN_CACHE.get(token);
  const saved = await redisLoad(token);
  if (!saved) return null;
  const entry = { createdAt: saved.createdAt, lastUsed: saved.lastUsed, reqCount: saved.reqCount,
                  rateWin: [], piKey: saved.piKey || '', piSecret: saved.piSecret || '', taddyKey: saved.taddyKey || '' };
  TOKEN_CACHE.set(token, entry);
  return entry;
}

function checkRateLimit(entry) {
  const now = Date.now();
  entry.rateWin = (entry.rateWin || []).filter(t => now - t < RATE_WINDOW_MS);
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now); entry.lastUsed = now; entry.reqCount = (entry.reqCount || 0) + 1;
  return true;
}

async function tokenMiddleware(req, res, next) {
  const entry = await getTokenEntry(req.params.token);
  if (!entry) return res.status(404).json({ error: 'Invalid token.' });
  if (!checkRateLimit(entry)) return res.status(429).json({ error: 'Rate limit exceeded.' });
  
  // Check if user has either PI credentials OR Taddy key
  if ((!entry.piKey || !entry.piSecret) && !entry.taddyKey) {
    return res.status(403).json({ 
      error: 'No credentials found. Generate a new URL with Podcast Index API key/secret OR Taddy API key.' 
    });
  }
  
  req.tokenEntry = entry;
  if (entry.reqCount % 20 === 0) redisSave(req.params.token, entry);
  next();
}

function getBaseUrl(req) { return (req.headers['x-forwarded-proto'] || req.protocol) + '://' + req.get('host'); }
function cleanText(s)    { return String(s || '').replace(/\s+/g, ' ').trim(); }

// ─── Episode / feed URL cache ─────────────────────────────────────────────
const EPISODE_CACHE  = new Map();
const FEED_URL_CACHE = new Map();

function cacheEpisode(ep) { if (ep && ep.id) EPISODE_CACHE.set(String(ep.id), ep); }

// ─── Podcast Index API ────────────────────────────────────────────────────
function piHeaders(entry) {
  const unixTime = Math.floor(Date.now() / 1000);
  const hash     = crypto.createHash('sha1').update(entry.piKey + entry.piSecret + unixTime).digest('hex');
  return {
    'X-Auth-Key':   entry.piKey,
    'X-Auth-Date':  String(unixTime),
    'Authorization': hash,
    'User-Agent':   'EclipsePodcastAddon/2.1'
  };
}

async function piGet(entry, endpoint, params) {
  try {
    const r = await axios.get('https://api.podcastindex.org/api/1.0' + endpoint, {
      params, headers: piHeaders(entry), timeout: 12000
    });
    return r.data;
  } catch (e) { console.warn('[PI]', endpoint, e.message); return null; }
}

async function piValidate(piKey, piSecret) {
  const fakeEntry = { piKey, piSecret };
  const data = await piGet(fakeEntry, '/search/byterm', { q: 'test', max: 1 });
  return !!(data && (data.status === 'true' || data.status === true || Array.isArray(data.feeds)));
}

// ─── Taddy API ────────────────────────────────────────────────────────────
async function taddyGet(entry, endpoint, params = {}) {
  if (!entry.taddyKey) return null;
  try {
    const r = await axios.get('https://api.taddy.app/v2' + endpoint, {
      params: { ...params, apikey: entry.taddyKey },
      headers: { 'User-Agent': 'EclipsePodcastAddon/2.1' },
      timeout: 12000
    });
    return r.data;
  } catch (e) { console.warn('[Taddy]', endpoint, e.message); return null; }
}

async function taddyValidate(taddyKey) {
  const data = await taddyGet({ taddyKey }, '/podcasts');
  return !!(data && data.data && data.data.length > 0);
}

// ─── iTunes API (episode search only — no key required) ───────────────────
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36';

async function itunesGet(endpoint, params) {
  try {
    const r = await axios.get('https://itunes.apple.com' + endpoint, {
      params, headers: { 'User-Agent': UA, 'Accept': 'application/json' }, timeout: 12000
    });
    return r.data;
  } catch (e) { console.warn('[iTunes]', e.message); return null; }
}

// ─── Format helpers ───────────────────────────────────────────────────────
function artworkHd(url) {
  if (!url) return null;
  return url.replace(/\/\d+x\d+(bb|cc)\./, '/600x600bb.');
}

function detectFormat(url) {
  if (!url) return 'mp3';
  const u = url.toLowerCase().split('?')[0];
  if (u.endsWith('.m4a') || u.includes('/m4a/')) return 'm4a';
  if (u.endsWith('.aac')) return 'aac';
  if (u.endsWith('.ogg') || u.endsWith('.opus')) return 'ogg';
  return 'mp3';
}

function durationSec(ms) {
  if (!ms) return null;
  const n = Number(ms);
  return isNaN(n) ? null : Math.floor(n / 1000);
}

// Maps a Podcast Index episode item → Eclipse track
function mapPiEpisode(ep, showTitle) {
  const id  = 'pi_ep_' + String(ep.id);
  const obj = {
    id, title: cleanText(ep.title),
    artist:    cleanText(ep.feedAuthor || ep.feedTitle || showTitle || ''),
    album:     cleanText(ep.feedTitle  || showTitle || ''),
    duration:  typeof ep.duration === 'number' ? ep.duration : null,
    artworkURL: ep.image || ep.feedImage || null,
    streamURL:  ep.enclosureUrl || null,
    format:     detectFormat(ep.enclosureUrl || '')
  };
  cacheEpisode(obj);
  return obj;
}

// Maps a Taddy episode → Eclipse track
function mapTaddyEpisode(ep, showTitle = '') {
  const id = 'taddy_ep_' + String(ep.id);
  const obj = {
    id,
    title: cleanText(ep.title || ep.name),
    artist: cleanText(ep.podcast?.title || ep.podcast?.name || showTitle || ''),
    album:  cleanText(ep.podcast?.title || ep.podcast?.name || showTitle || ''),
    duration: ep.duration || ep.audio_length_sec || null,
    artworkURL: ep.podcast?.image || ep.image || null,
    streamURL:  ep.audio || ep.enclosure || ep.audio_url || null,
    format:     detectFormat(ep.audio || ep.enclosure || '')
  };
  cacheEpisode(obj);
  return obj;
}

// Maps a Podcast Index feed → Eclipse album
function mapPiFeed(f) {
  return {
    id:         'pi_' + String(f.id),
    title:      cleanText(f.title),
    artist:     cleanText(f.author || f.ownerName || ''),
    artworkURL: f.image || f.artwork || null,
    trackCount: f.episodeCount  || null,
    year:       f.newestItemPublishTime ? String(new Date(f.newestItemPublishTime * 1000).getFullYear()) : null
  };
}

// Maps a Taddy podcast → Eclipse album
function mapTaddyPodcast(pod) {
  return {
    id:         'taddy_' + String(pod.id),
    title:      cleanText(pod.title || pod.name),
    artist:     cleanText(pod.author || pod.producer || ''),
    artworkURL: pod.image || pod.artwork || null,
    trackCount: pod.episodes_count || pod.total_episodes || null,
    year:       pod.first_episode_pub_date ? String(new Date(pod.first_episode_pub_date).getFullYear()) : null
  };
}

// Maps an iTunes episode → Eclipse track (for episode-level text search)
function mapItunesEpisode(ep) {
  const id  = 'ep_' + String(ep.trackId);
  const obj = {
    id, title: cleanText(ep.trackName),
    artist:    cleanText(ep.collectionName || ep.artistName),
    album:     cleanText(ep.collectionName),
    duration:  durationSec(ep.trackTimeMillis),
    artworkURL: artworkHd(ep.artworkUrl600 || ep.artworkUrl160),
    streamURL:  ep.episodeUrl || null,
    format:     detectFormat(ep.episodeUrl || '')
  };
  cacheEpisode(obj);
  return obj;
}

// ─── RSS helpers ──────────────────────────────────────────────────────────
function rssTag(block, tag) {
  let m = block.match(new RegExp('<' + tag + '[^>]*>\\s*<!\\\[CDATA\\[([\\s\\S]*?)\\\]\\]>\\s*<\\/' + tag + '>', 'i'));
  if (m) return m[1].trim();
  m = block.match(new RegExp('<' + tag + '[^>]*>([\\s\\S]*?)<\\/' + tag + '>', 'i'));
  if (m) return m[1].replace(/<[^>]*>/g, '').trim();
  return '';
}

function rssAttr(block, tag, attr) {
  const m = block.match(new RegExp('<' + tag + '[^>]+' + attr + '=["\']([^"\'\\s>]*)["\']', 'i'));
  return m ? m[1] : '';
}

function parseDuration(s) {
  if (!s) return null;
  const parts = String(s).trim().split(':').map(Number);
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  if (parts.length === 1 && !isNaN(parts[0])) return parts[0];
  return null;
}

async function fetchRss(feedUrl) {
  try {
    const r = await axios.get(feedUrl, {
      headers: { 'User-Agent': UA, 'Accept': 'application/rss+xml, application/xml, text/xml, */*' },
      timeout: 15000, responseType: 'text', validateStatus: s => s < 500
    });
    return r.data || null;
  } catch (e) { return null; }
}

function parseRssToShow(xml) {
  if (!xml) return null;
  const chanMatch = xml.match(/<channel[^>]*>([\s\S]*?)<\/channel>/i);
  const chan       = chanMatch ? chanMatch[1] : xml;
  const chanNoItems = chan.replace(/<item[\s\S]*$/i, '');
  const showTitle  = rssTag(chanNoItems, 'title');
  const showArtist = rssTag(chan, 'itunes:author') || rssTag(chan, 'managingEditor') || '';
  const showArt    = rssAttr(chan, 'itunes:image', 'href') || '';
  const showDesc   = rssTag(chanNoItems, 'description') || rssTag(chanNoItems, 'itunes:summary') || '';

  const items = []; const itemRx = /<item[^>]*>([\s\S]*?)<\/item>/gi; let m;
  while ((m = itemRx.exec(xml)) !== null) items.push(m[1]);

  const tracks = items.map((item, i) => {
    const encUrl = rssAttr(item, 'enclosure', 'url');
    if (!encUrl) return null;
    const guid = rssTag(item, 'guid') || encUrl;
    const hash = crypto.createHash('md5').update(guid).digest('hex').slice(0, 12);
    const id   = 'rss_ep_' + hash;
    const obj  = {
      id, title: cleanText(rssTag(item, 'title')) || ('Episode ' + (i + 1)),
      artist: cleanText(showArtist) || cleanText(showTitle),
      album:  cleanText(showTitle),
      duration:   parseDuration(rssTag(item, 'itunes:duration')),
      artworkURL: rssAttr(item, 'itunes:image', 'href') || showArt || null,
      streamURL:  encUrl,
      format:     detectFormat(encUrl)
    };
    cacheEpisode(obj);
    return obj;
  }).filter(Boolean);

  return { title: cleanText(showTitle), artist: cleanText(showArtist), artworkURL: showArt || null, description: cleanText(showDesc).slice(0, 300), tracks };
}

// ─── Config page ──────────────────────────────────────────────────────────
function buildConfigPage(baseUrl) {
  let h = '';
  h += '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">';
  h += '<meta name="viewport" content="width=device-width,initial-scale=1">';
  h += '<title>Eclipse – Podcast Addon</title>';
  h += '<style>*{box-sizing:border-box;margin:0;padding:0}';
  h += 'body{background:#0c0c0f;color:#e8e8e8;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px}';
  h += '.logo{margin-bottom:20px}';
  h += '.card{background:#131316;border:1px solid #1f1f26;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.55);margin-bottom:20px}';
  h += 'h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}';
  h += 'p.sub{font-size:14px;color:#777;margin-bottom:20px;line-height:1.6}';
  h += '.tip{background:#0d1520;border:1px solid #1a2d42;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#5a8abe;line-height:1.7}.tip b{color:#7cb8de}';
  h += '.tip a{color:#7cb8de}';
  h += '.pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}';
  h += '.pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#0d1a2a;color:#4a9eff;border:1px solid #1a3a5e}';
  h += '.pill.g{background:#0d1f0d;color:#6db86d;border-color:#2d422a}';
  h += '.lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#555;margin-bottom:8px;margin-top:16px}';
  h += 'input{width:100%;background:#0c0c0f;border:1px solid #222;border-radius:10px;color:#e8e8e8;font-size:14px;padding:12px 14px;margin-bottom:6px;outline:none;transition:border-color .15s;font-family:ui-monospace,monospace}';
  h += 'input:focus{border-color:#4a9eff}input::placeholder{color:#333;font-family:-apple-system,sans-serif}';
  h += '.hint{font-size:12px;color:#484848;margin-bottom:12px;line-height:1.7}.hint a{color:#4a9eff;text-decoration:none}.hint code{background:#1a1a1a;padding:1px 5px;border-radius:4px;color:#888}';
  h += 'button{cursor:pointer;border:none;border-radius:10px;font-size:15px;font-weight:700;padding:13px;width:100%;margin-top:6px;margin-bottom:12px;transition:background .15s}';
  h += '.bo{background:#4a9eff;color:#fff}.bo:hover{background:#2a7fdf}.bo:disabled{background:#252525;color:#444;cursor:not-allowed}';
  h += '.bg{background:#1a4a20;color:#e8e8e8;border:1px solid #2a6a30}.bg:hover{background:#245c2a}.bg:disabled{background:#252525;color:#444;cursor:not-allowed}';
  h += '.bd{background:#1a1a1a;color:#aaa;border:1px solid #222;font-size:13px;padding:10px}.bd:hover{background:#222;color:#fff}';
  h += '.box{display:none;background:#0c0c0f;border:1px solid #1e1e2e;border-radius:12px;padding:18px;margin-bottom:14px}';
  h += '.blbl{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}';
  h += '.burl{font-size:12px;color:#4a9eff;word-break:break-all;font-family:"SF Mono",monospace;margin-bottom:14px;line-height:1.5}';
  h += 'hr{border:none;border-top:1px solid #1a1a1a;margin:24px 0}';
  h += '.steps{display:flex;flex-direction:column;gap:12px}.step{display:flex;gap:12px;align-items:flex-start}';
  h += '.sn{background:#1a1a1a;border:1px solid #252525;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#666}';
  h += '.st{font-size:13px;color:#666;line-height:1.6}.st b{color:#aaa}';
  h += '.warn{background:#14100a;border:1px solid #2e2000;border-radius:10px;padding:14px;margin-top:20px;font-size:12px;color:#8a6a30;line-height:1.7}';
  h += '.badge{display:inline-block;background:#0d1a2a;color:#4a9eff;border:1px solid #1a3a5e;border-radius:20px;font-size:11px;font-weight:600;padding:3px 10px;margin-bottom:14px}';
  h += '.status{font-size:13px;color:#666;margin:8px 0;min-height:18px}.status.ok{color:#5a9e5a}.status.err{color:#c0392b}.status.spin{color:#4a9eff}';
  h += '.kstep{background:#0a0e16;border:1px solid #1a2a3e;border-radius:12px;padding:16px;margin-bottom:12px}';
  h += '.kstep-title{font-size:12px;font-weight:700;color:#4a9eff;margin-bottom:8px;text-transform:uppercase;letter-spacing:.06em}';
  h += '.kstep p{font-size:13px;color:#667;line-height:1.7}.kstep a{color:#4a9eff;text-decoration:none;font-weight:600}';
  h += '.row2{display:grid;grid-template-columns:1fr 1fr;gap:10px}';
  h += '.row3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}';
  h += '.preview{background:#0c0c0f;border:1px solid #1a1a1a;border-radius:10px;padding:12px;max-height:200px;overflow-y:auto;margin-bottom:12px;display:none}';
  h += '.tr{display:flex;gap:10px;align-items:center;padding:5px 0;border-bottom:1px solid #181818;font-size:13px}.tr:last-child{border-bottom:none}';
  h += '.tn{color:#444;font-size:11px;min-width:22px;text-align:right}.ti{flex:1;min-width:0}';
  h += '.tt{color:#e8e8e8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.ta{color:#666;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
  h += 'footer{margin-top:32px;font-size:12px;color:#333;text-align:center;line-height:1.8}';
  h += '</style></head><body>';

  // Logo
  h += '<svg class="logo" width="52" height="52" viewBox="0 0 52 52" fill="none">';
  h += '<circle cx="26" cy="26" r="26" fill="#1a3a5e"/>';
  h += '<rect x="20" y="10" width="12" height="22" rx="6" fill="#4a9eff"/>';
  h += '<path d="M14 28c0 6.627 5.373 12 12 12s12-5.373 12-12" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round" fill="none"/>';
  h += '<line x1="26" y1="40" x2="26" y2="45" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round"/>';
  h += '<line x1="20" y1="45" x2="32" y2="45" stroke="#4a9eff" stroke-width="2.5" stroke-linecap="round"/>';
  h += '</svg>';

  // ── Card 1: credentials + generate ──────────────────────────────────────
  h += '<div class="card">';
  h += '<h1>Podcasts for Eclipse</h1>';
  h += '<p class="sub">Choose Podcast Index (free) OR Taddy API. Both unlock 4M+ podcasts. Takes 2 minutes.</p>';
  h += '<div class="pills"><span class="pill">Episodes & shows</span><span class="pill">Creator pages</span><span class="pill">Playlists</span><span class="pill g">4M+ podcasts</span><span class="pill g">Offline download</span></div>';

  // Step A: get credentials
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 1 — Choose your API</div>';
  h += '<p><b>Podcast Index (Free):</b> Go to <a href="https://api.podcastindex.org" target="_blank">api.podcastindex.org</a>, create account, copy API Key + Secret.</p>';
  h += '<p><b>Taddy API:</b> Get key from <a href="https://taddy.app" target="_blank">taddy.app</a> dashboard.</p>';
  h += '</div>';

  // Credentials inputs
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 2 — Enter credentials</div>';
  h += '<div class="row3">';
  h += '<div><div class="lbl">Podcast Index Key</div><input type="text" id="piKey" placeholder="ZQEW8D5ET2L6..."></div>';
  h += '<div><div class="lbl">Podcast Index Secret</div><input type="password" id="piSecret" placeholder="Your secret..."></div>';
  h += '<div><div class="lbl">OR Taddy API Key</div><input type="text" id="taddyKey" placeholder="taddy_xxxxx..."></div>';
  h += '</div>';
  h += '<div class="hint">Enter <b>either</b> Podcast Index credentials <b>OR</b> Taddy key (not both needed).</div>';
  h += '<div class="status" id="credStatus"></div>';
  h += '</div>';

  // Generate
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 3 — Generate addon URL</div>';
  h += '<button class="bo" id="genBtn" onclick="generate()" disabled>Enter API credentials first</button>';
  h += '<div class="box" id="genBox"><div class="blbl">Your addon URL — paste into Eclipse</div><div class="burl" id="genUrl"></div><button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button></div>';
  h += '</div>';

  h += '<hr>';

  // Refresh existing
  h += '<div class="lbl">Restore existing URL</div>';
  h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
  h += '<button class="bg" id="refBtn" onclick="doRefresh()">Restore Existing URL</button>';
  h += '<div class="box" id="refBox"><div class="blbl">Restored — same URL, still works</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';

  h += '<hr>';
  h += '<div class="steps">';
  h += '<div class="step"><div class="sn">4</div><div class="st">Copy URL above</div></div>';
  h += '<div class="step"><div class="sn">5</div><div class="st">Eclipse → Settings → Connections → Addon</div></div>';
  h += '<div class="step"><div class="sn">6</div><div class="st">Paste URL → Install</div></div>';
  h += '</div>';
  h += '<div class="warn">Credentials survive restarts via Redis. Only reinstall if domain changes.</div>';
  h += '</div>';

  // ── Importer ────────────────────────────────────────────────────────────
  h += '<div class="card">';
  h += '<span class="badge">Podcast Importer</span>';
  h += '<h2>Import Podcast to Library</h2>';
  h += '<p class="sub">Downloads CSV for Eclipse Library → Import CSV.</p>';
  h += '<div class="lbl">Your Addon URL</div>';
  h += '<input type="text" id="impToken" placeholder="Auto-fills after generating">';
  h += '<div class="lbl">Podcast URL</div>';
  h += '<input type="text" id="impUrl" placeholder="podcasts.apple.com/... or RSS feed">';
  h += '<button class="bg" id="impBtn" onclick="doImport()">Fetch & Download CSV</button>';
  h += '<div class="status" id="impStatus"></div>';
  h += '<div class="preview" id="impPreview"></div>';
  h += '</div>';

  h += '<footer>Eclipse Podcast Addon v2.1.0 • Podcast Index + Taddy API • <a href="' + baseUrl + '/health">' + baseUrl + '</a></footer>';

  // ── Client JS ───────────────────────────────────────────────────────────
  h += '<script>';
  h += 'var _gu="",_ru="";';
  
  // credential change
  h += 'function onCredChange(){';
  h += 'var pik=document.getElementById("piKey").value.trim(),pis=document.getElementById("piSecret").value.trim(),tk=document.getElementById("taddyKey").value.trim();';
  h += 'var btn=document.getElementById("genBtn");';
  h += 'if((pik&&pis)||tk){btn.disabled=false;btn.textContent="Generate My Addon URL";}else{btn.disabled=true;btn.textContent="Enter API credentials first";}';
  h += '}';
  
  // Add input listeners
  h += 'document.getElementById("piKey").oninput=document.getElementById("piSecret").oninput=document.getElementById("taddyKey").oninput=onCredChange;';
  
  // generate
  h += 'function generate(){';
  h += 'var pik=document.getElementById("piKey").value.trim(),pis=document.getElementById("piSecret").value.trim(),tk=document.getElementById("taddyKey").value.trim();';
  h += 'if(!pik&&!pis&&!tk){alert("Enter credentials.");return;}';
  h += 'var btn=document.getElementById("genBtn"),st=document.getElementById("credStatus");';
  h += 'btn.disabled=true;btn.textContent="Validating...";st.className="status spin";st.textContent="Checking API keys...";';
  h += 'fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({piKey:pik,piSecret:pis,taddyKey:tk})})'
    .then(r=>r.json())
    .then(d=>{
      if(d.error){st.className="status err";st.textContent=d.error;btn.disabled=false;btn.textContent="Generate URL";return;}
      _gu=d.manifestUrl;
      document.getElementById("genUrl").textContent=_gu;
      document.getElementById("genBox").style.display="block";
      document.getElementById("impToken").value=_gu;
      st.className="status ok";st.textContent="✓ Credentials valid — URL ready";
      btn.disabled=false;btn.textContent="Regenerate";
    })
    .catch(e=>{st.className="status err";st.textContent="Error: "+e;btn.disabled=false;btn.textContent="Generate URL";});
  h += '}';
  
  // copy, refresh, import functions (unchanged from original)
  h += 'function copyGen(){if(!_gu)return;navigator.clipboard.writeText(_gu).then(()=>{var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(()=>{b.textContent="Copy URL";},1500);});}';
  h += 'function doRefresh(){var btn=document.getElementById("refBtn"),eu=document.getElementById("existingUrl").value.trim();if(!eu){alert("Paste URL.");return;}btn.disabled=true;btn.textContent="Checking...";fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})}).then(r=>r.json()).then(d=>{if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Restore";return;}_ru=d.manifestUrl;document.getElementById("refUrl").textContent=_ru;document.getElementById("refBox").style.display="block";document.getElementById("impToken").value=_ru;btn.disabled=false;btn.textContent="Restore Again";}).catch(e=>{alert("Error: "+e);btn.disabled=false;btn.textContent="Restore";});}';
  h += 'function copyRef(){if(!_ru)return;navigator.clipboard.writeText(_ru).then(()=>{var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(()=>{b.textContent="Copy URL";},1500);});}';
  h += 'function getTok(s){var m=s.match(/\/u\/([a-f0-9]{28})\//);return m?m[1]:null;}';
  h += 'function hesc(s){return String(s||"").replace(/&/g,"&").replace(/</g,"<").replace(/>/g,">");}';
  h += 'function doImport(){var btn=document.getElementById("impBtn"),raw=document.getElementById("impToken").value.trim(),purl=document.getElementById("impUrl").value.trim(),st=document.getElementById("impStatus"),pv=document.getElementById("impPreview");if(!raw||!purl){st.className="status err";st.textContent="Enter URL and podcast.";return;}var tok=getTok(raw);if(!tok){st.className="status err";st.textContent="Invalid token in URL.";return;}btn.disabled=true;btn.textContent="Fetching...";st.className="status spin";st.textContent="Fetching episodes...";fetch("/u/"+tok+"/import?url="+encodeURIComponent(purl)).then(r=>{if(!r.ok)return r.json().then(e=>Promise.reject(e));return r.json();}).then(data=>{var tracks=data.tracks||[];if(!tracks.length)throw new Error("No episodes.");var rows=tracks.slice(0,50).map((t,i)=>\'<div class="tr"><span class="tn">\'+(i+1)+\'</span><div class="ti"><div class="tt">\'+hesc(t.title polished)+\'</div><div class="ta">\'+hesc(t.artist)+\'</div></div></div>\').join("");if(tracks.length>50)rows+=\'<div class="tr" style="text-align:center;color:#555">+\'+(tracks.length-50)+\' more</div>\';pv.innerHTML=rows;pv.style.display="block";st.className="status ok";st.textContent="Found "+tracks.length+" episodes";var lines=["Title,Artist,Album,Duration"];tracks.forEach(t=>{function ce(s){s=String(s||"");if(s.indexOf(",")!==-1||s.indexOf(\'"\' )!==-1){s=\'"\'+s.replace(/"/g,\'""\')+\'"\'}return s;}lines.push(ce(t.title)+","+ce(t.artist)+","+ce(data.title||"")+","+ce(t.duration||""));});var blob=new Blob([lines.join("\\n")],{type:"text/csv"});var a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=(data.title||"podcast").replace(/[^a-zA-Z0-9 _-]/g,"").trim()+".csv";document.body.appendChild(a);a.click();document.body.removeChild(a);btn.disabled=false;btn.textContent="Download CSV";}).catch(e=>{st.className="status err";st.textContent=e.message;btn.disabled=false;btn.textContent="Download CSV";});}';
  h += '<\/script></body></html>';
  return h;
}

// ─── Routes ───────────────────────────────────────────────────────────────
app.get('/', function (req, res) {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(buildConfigPage(getBaseUrl(req)));
});

app.post('/generate', async function (req, res) {
  const ip     = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();
  const piKey  = cleanText((req.body && req.body.piKey)    || '');
  const piSec  = cleanText((req.body && req.body.piSecret) || '');
  const taddyKey = cleanText((req.body && req.body.taddyKey) || '');

  // Validate at least one set of credentials
  let valid = false;
  if (piKey && piSec) valid = await piValidate(piKey, piSec);
  else if (taddyKey) valid = await taddyValidate(taddyKey);
  
  if (!valid) return res.status(401).json({ error: 'Invalid credentials. Check Podcast Index or Taddy API key.' });

  const bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens today from this IP.' });

  const token = generateToken();
  const entry = { 
    createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [], 
    piKey, piSecret: piSec, taddyKey 
  };
  TOKEN_CACHE.set(token, entry);
  await redisSave(token, entry);
  bucket.count++;

  res.json({ token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json' });
});

app.post('/refresh', async function (req, res) {
  let raw = (req.body && req.body.existingUrl) ? String(req.body.existingUrl).trim() : '';
  let token = raw;
  const m = raw.match(/\/u\/([a-f0-9]{28})\//);
  if (m) token = m[1];
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste full addon URL.' });
  
  const entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate new one.' });
  
  res.json({ token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

// ─── Manifest ─────────────────────────────────────────────────────────────
app.get('/u/:token/manifest.json', tokenMiddleware, function (req, res) {
  res.json({
    id:          'com.eclipse.podcasts.' + req.params.token.slice(0, 8),
    name:        'Podcasts (PI+Taddy)',
    version:     '2.1.0',
    description: '4M+ podcasts via Podcast Index + Taddy API.',
    icon:        'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRm7Pco873CnlEKMoATgv0rNfOXQNdHg4strPErJftrlg&s=10',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  });
});

// ─── Search (PI + Taddy + iTunes) ─────────────────────────────────────────
app.get('/u/:token/search', tokenMiddleware, async function (req, res) {
  const q     = cleanText(req.query.q);
  const entry = req.tokenEntry;
  if (!q) return res.json({ tracks: [], albums: [], artists: [], playlists: [] });

  try {
    // Parallel searches across all APIs
    const [piData, taddyData, itunesData] = await Promise.allSettled([
      entry.piKey ? piGet(entry, '/search/byterm', { q, max: 15 }) : Promise.resolve(null),
      taddyGet(entry, '/podcasts?q=' + encodeURIComponent(q) + '&size=15'),
      itunesGet('/search', { term: q, media: 'podcast', entity: 'podcastEpisode', limit: 20, explicit: 'Yes' })
    ]);

    const piResult = piData.status === 'fulfilled' ? piData.value : null;
    const taddyResult = taddyData.status === 'fulfilled' ? taddyData.value : null;
    const itunesResult = itunesData.status === 'fulfilled' ? itunesData.value : null;

    const piFeeds = (piResult && piResult.feeds) || [];
    const taddyPodcasts = (taddyResult && taddyResult.data) || [];

    // Tracks: iTunes episodes + Taddy episodes + PI episodes
    const itunesTracks = ((itunesResult && itunesResult.results) || [])
      .filter(ep => ep.kind === 'podcast-episode' && ep.episodeUrl)
      .map(mapItunesEpisode);
    
    const taddyTracks = taddyPodcasts.slice(0, 10).flatMap(pod => 
      (pod.episodes || []).slice(0, 2).map(ep => mapTaddyEpisode(ep, pod.title))
    ).filter(t => t.streamURL);

    const tracks = [...itunesTracks, ...taddyTracks].slice(0, 25);

    // Albums: PI feeds + Taddy podcasts
    const albums = [...piFeeds.map(mapPiFeed), ...taddyPodcasts.slice(0, 10).map(mapTaddyPodcast)].slice(0, 15);

    // Artists: from PI (Taddy doesn't have dedicated creator search)
    const artistMap = new Map();
    piFeeds.forEach(f => {
      const key = cleanText(f.author || f.ownerName || '').toLowerCase();
      if (!key) return;
      if (!artistMap.has(key)) {
        artistMap.set(key, {
          id:         'pi_author_' + Buffer.from(cleanText(f.author || f.ownerName || '')).toString('base64url'),
          name:       cleanText(f.author || f.ownerName || ''),
          artworkURL: f.image || f.artwork || null,
          genres:     f.categories ? Object.values(f.categories).slice(0, 2) : []
        });
      }
    });
    const artists = Array.from(artistMap.values()).slice(0, 6);

    // Playlists: PI feeds with URLs
    const playlists = piFeeds
      .filter(f => f.url)
      .slice(0, 8)
      .map(f => {
        const feedId = 'rss_' + Buffer.from(f.url).toString('base64url');
        FEED_URL_CACHE.set(feedId, f.url);
        return {
          id:          feedId,
          title:       cleanText(f.title),
          creator:     cleanText(f.author || f.ownerName || ''),
          artworkURL:  f.image || f.artwork || null,
          trackCount:  f.episodeCount || null,
          description: cleanText(f.description || '').slice(0, 200)
        };
      });

    res.json({ tracks, albums, artists, playlists });
  } catch (e) {
    console.error('[search]', e.message);
    res.status(500).json({ error: 'Search failed', tracks: [], albums: [], artists: [], playlists: [] });
  }
});

// ─── Stream (handles pi_ep_, taddy_ep_, ep_, rss_ep_) ─────────────────────
app.get('/u/:token/stream/:id', tokenMiddleware, async function (req, res) {
  const id    = req.params.id;
  const entry = req.tokenEntry;

  // Check cache first
  const cached = EPISODE_CACHE.get(id);
  if (cached && cached.streamURL) return res.json({ url: cached.streamURL, format: cached.format || 'mp3' });

  // Taddy episode
  if (id.startsWith('taddy_ep_')) {
    const epId = id.replace('taddy_ep_', '');
    const data = await taddyGet(entry, '/episodes/' + epId);
    if (data && data.data && data.data.audio) {
      return res.json({ url: data.data.audio, format: detectFormat(data.data.audio) });
    }
  }

  // PI episode
  if (id.startsWith('pi_ep_')) {
    const epId = id.replace('pi_ep_', '');
    const data = await piGet(entry, '/episodes/byid', { id: epId });
    if (data && data.episode && data.episode.enclosureUrl) {
      const ep = mapPiEpisode(data.episode, '');
      return res.json({ url: ep.streamURL, format: ep.format });
    }
  }

  // iTunes episode
  if (id.startsWith('ep_')) {
    const trackId = id.replace('ep_', '');
    const data    = await itunesGet('/lookup', { id: trackId });
    if (data && data.results) {
      const ep = data.results.find(r => r.kind === 'podcast-episode' && r.episodeUrl);
      if (ep) { const mapped = mapItunesEpisode(ep); return res.json({ url: mapped.streamURL, format: mapped.format }); }
    }
  }

  return res.status(404).json({ error: 'Stream not found: ' + id });
});

// ─── Album (PI + Taddy) ───────────────────────────────────────────────────
app.get('/u/:token/album/:id', tokenMiddleware, async function (req, res) {
  const rawId  = req.params.id;
  const entry  = req.tokenEntry;

  try {
    let tracks = [];
    let albumInfo = { title: '', artist: '', artworkURL: null, description: '', trackCount: 0 };

    // PI podcast
    if (rawId.startsWith('pi_')) {
      const feedId = rawId.replace('pi_', '');
      const [feedData, epData] = await Promise.all([
        piGet(entry, '/podcasts/byfeedid', { id: feedId }),
        piGet(entry, '/episodes/byfeedid', { id: feedId, max: 200, fulltext: true })
      ]);
      
      const feed   = (feedData && feedData.feed) || {};
      tracks = ((epData && epData.items) || []).map(ep => mapPiEpisode(ep, feed.title || ''));
      albumInfo = {
        id: rawId, title: cleanText(feed.title || ''), artist: cleanText(feed.author || feed.ownerName || ''),
        artworkURL: feed.image || feed.artwork || null,
        year: feed.newestItemPublishTime ? String(new Date(feed.newestItemPublishTime * 1000).getFullYear()) : null,
        description: cleanText(feed.description || '').slice(0, 500),
        trackCount: tracks.length, tracks
      };
    }
    // Taddy podcast
    else if (rawId.startsWith('taddy_')) {
      const podId = rawId.replace('taddy_', '');
      const data = await taddyGet(entry, '/podcasts/' + podId + '?include_episodes=true&size=200');
      const pod = data?.data || {};
      tracks = (pod.episodes || []).map(ep => mapTaddyEpisode(ep, pod.title));
      albumInfo = {
        id: rawId, title: cleanText(pod.title || pod.name || ''),
        artist: cleanText(pod.author || pod.producer || ''),
        artworkURL: pod.image || pod.artwork || null,
        year: pod.first_episode_pub_date ? String(new Date(pod.first_episode_pub_date).getFullYear()) : null,
        description: cleanText(pod.description || '').slice(0, 500),
        trackCount: tracks.length, tracks
      };
    }

    res.json(albumInfo);
  } catch (e) {
    console.error('[album]', e.message);
    res.status(500).json({ error: 'Album fetch failed.' });
  }
});

// ─── Artist, Playlist, Import routes remain the same as original ──────────
app.get('/u/:token/artist/:id', tokenMiddleware, async function (req, res) {
  // Same as original PI artist implementation
  const rawId = req.params.id;
  const entry = req.tokenEntry;

  try {
    const authorName = Buffer.from(rawId.replace('pi_author_', ''), 'base64url').toString('utf8');
    const data   = await piGet(entry, '/search/byterm', { q: authorName, max: 20 });
    const feeds  = ((data && data.feeds) || []);

    const exact   = feeds.filter(f => cleanText(f.author || '').toLowerCase() === authorName.toLowerCase());
    const matched = exact.length > 0 ? exact : feeds.slice(0, 10);
    const first   = matched[0] || {};

    let topTracks = [];
    if (first.id) {
      try {
        const epData  = await piGet(entry, '/episodes/byfeedid', { id: first.id, max: 10 });
        topTracks = ((epData && epData.items) || []).map(ep => mapPiEpisode(ep, first.title || ''));
      } catch (e2) { /* skip */ }
    }

    res.json({
      id: rawId,
      name: authorName,
      artworkURL: first.image || first.artwork || null,
      bio: null,
      genres: first.categories ? Object.values(first.categories).slice(0, 2) : [],
      topTracks,
      albums: matched.map(mapPiFeed)
    });
  } catch (e) {
    console.error('[artist]', e.message);
    res.status(500).json({ error: 'Artist fetch failed.' });
  }
});

app.get('/u/:token/playlist/:id', tokenMiddleware, async function (req, res) {
  // Same as original RSS playlist implementation
  const rawId = req.params.id;
  if (!rawId.startsWith('rss_')) return res.status(404).json({ error: 'Playlist not found.' });

  let feedUrl = FEED_URL_CACHE.get(rawId) || null;
  if (!feedUrl) {
    try { feedUrl = Buffer.from(rawId.slice(4), 'base64url').toString('utf8'); }
    catch (e) { return res.status(400).json({ error: 'Invalid playlist ID.' }); }
  }

  const xml = await fetchRss(feedUrl);
  if (!xml) return res.status(404).json({ error: 'Could not fetch RSS feed.' });
  const show = parseRssToShow(xml);
  if (!show) return res.status(500).json({ error: 'Could not parse RSS feed.' });

  res.json({ id: rawId, title: show.title, description: show.description, artworkURL: show.artworkURL, creator: show.artist, tracks: show.tracks });
});

app.get('/u/:token/import', tokenMiddleware, async function (req, res) {
  // Same as original import implementation
  const inputUrl = cleanText(req.query.url);
  if (!inputUrl) return res.status(400).json({ error: 'Pass ?url= with podcast or RSS URL.' });

  const appleMatch = inputUrl.match(/\/id(\d{6,12})/);
  if (appleMatch) {
    const podId = appleMatch[1];
    const data  = await itunesGet('/lookup', { id: podId, entity: 'podcastEpisode', limit: 300 });
    if (!data || !data.results || data.results.length === 0) return res.status(404).json({ error: 'Podcast not found.' });
    const show     = data.results.find(r => r.wrapperType === 'collection') || data.results[0];
    const episodes = data.results.filter(r => r.kind === 'podcast-episode' && r.episodeUrl);
    return res.json({ title: cleanText(show.collectionName || show.trackName || 'Podcast'), artworkURL: artworkHd(show.artworkUrl600), tracks: episodes.map(mapItunesEpisode) });
  }

  if (/^https?:\/\//i.test(inputUrl)) {
    const xml = await fetchRss(inputUrl);
    if (!xml) return res.status(404).json({ error: 'Could not fetch RSS.' });
    const parsed = parseRssToShow(xml);
    if (!parsed) return res.status(500).json({ error: 'Could not parse RSS.' });
    return res.json({ title: parsed.title, artworkURL: parsed.artworkURL, tracks: parsed.tracks });
  }

  return res.status(400).json({ error: 'Use Apple Podcasts URL or RSS feed URL.' });
});

// ─── Health ───────────────────────────────────────────────────────────────
app.get('/health', function (req, res) {
  res.json({ 
    status: 'ok', 
    version: '2.1.0', 
    redisConnected: !!(redis && redis.status === 'ready'), 
    activeTokens: TOKEN_CACHE.size, 
    cachedEpisodes: EPISODE_CACHE.size, 
    cachedFeeds: FEED_URL_CACHE.size, 
    timestamp: new Date().toISOString() 
  });
});

app.listen(PORT, () => console.log('Eclipse Podcast Addon v2.1.0 (PI+Taddy) on port ' + PORT));
