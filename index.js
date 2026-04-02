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
      piKey: entry.piKey, piSecret: entry.piSecret
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
                  rateWin: [], piKey: saved.piKey || '', piSecret: saved.piSecret || '' };
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
  if (!entry.piKey || !entry.piSecret) return res.status(403).json({ error: 'No Podcast Index credentials on this token. Generate a new URL with your API key and secret.' });
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
    'User-Agent':   'EclipsePodcastAddon/2.0'
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
  let m = block.match(new RegExp('<' + tag + '[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>\\s*<\\/' + tag + '>', 'i'));
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
  h += '<title>Eclipse \u2013 Podcast Addon</title>';
  h += '<style>';
  h += '*{box-sizing:border-box;margin:0;padding:0}';
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
  h += '<p class="sub">Each user brings their own free Podcast Index API key so nobody shares rate limits. It takes 2 minutes to set up.</p>';
  h += '<div class="pills"><span class="pill">Episodes &amp; shows</span><span class="pill">Creator pages</span><span class="pill">Playlists</span><span class="pill g">4M+ podcasts</span><span class="pill g">Offline download</span></div>';

  // Step A: get credentials
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 1 &mdash; Get your free Podcast Index credentials</div>';
  h += '<p>Go to <a href="https://api.podcastindex.org" target="_blank">api.podcastindex.org</a>, click <b>Create Account</b>, sign up, then copy your <b>API Key</b> and <b>API Secret</b> from the dashboard.</p>';
  h += '</div>';

  // Step B: enter credentials
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 2 &mdash; Enter your credentials below</div>';
  h += '<div class="row2">';
  h += '<div><div class="lbl">API Key</div><input type="text" id="piKey" placeholder="ZQEW8D5ET2L6..." oninput="onCredChange()"></div>';
  h += '<div><div class="lbl">API Secret</div><input type="password" id="piSecret" placeholder="Your secret..." oninput="onCredChange()"></div>';
  h += '</div>';
  h += '<div class="hint">Your credentials are stored on this server with your token. Each user has their own &mdash; no shared rate limits.</div>';
  h += '<div class="status" id="credStatus"></div>';
  h += '</div>';

  // Step C: generate
  h += '<div class="kstep">';
  h += '<div class="kstep-title">Step 3 &mdash; Generate your addon URL</div>';
  h += '<button class="bo" id="genBtn" onclick="generate()" disabled>Enter your API credentials first</button>';
  h += '<div class="box" id="genBox"><div class="blbl">Your addon URL &mdash; paste into Eclipse</div><div class="burl" id="genUrl"></div><button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button></div>';
  h += '</div>';

  h += '<hr>';

  // Refresh
  h += '<div class="lbl">Restore an existing URL</div>';
  h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
  h += '<div class="hint">Already have a URL? Paste it above to confirm it still works. Your credentials are saved to Redis.</div>';
  h += '<button class="bg" id="refBtn" onclick="doRefresh()">Restore Existing URL</button>';
  h += '<div class="box" id="refBox"><div class="blbl">Restored &mdash; same URL, still works in Eclipse</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';

  h += '<hr>';
  h += '<div class="steps">';
  h += '<div class="step"><div class="sn">4</div><div class="st">Copy the URL above</div></div>';
  h += '<div class="step"><div class="sn">5</div><div class="st">Open <b>Eclipse</b> &rarr; Settings &rarr; Connections &rarr; Add Connection &rarr; Addon</div></div>';
  h += '<div class="step"><div class="sn">6</div><div class="st">Paste your URL and tap <b>Install</b></div></div>';
  h += '</div>';
  h += '<div class="warn">Your URL and credentials survive server restarts via Redis. Never reinstall the addon unless the domain changes.</div>';
  h += '</div>';

  // ── Card 2: importer ─────────────────────────────────────────────────────
  h += '<div class="card">';
  h += '<span class="badge">Podcast Importer</span>';
  h += '<h2>Import a Podcast to Eclipse Library</h2>';
  h += '<p class="sub">Downloads a CSV you can import via Library &rarr; Import CSV in Eclipse.</p>';
  h += '<div class="lbl">Your Addon URL</div>';
  h += '<input type="text" id="impToken" placeholder="Paste your addon URL (auto-fills after generating)">';
  h += '<div class="lbl">Podcast URL</div>';
  h += '<input type="text" id="impUrl" placeholder="https://podcasts.apple.com/...  or  any RSS feed URL">';
  h += '<div class="hint">Apple Podcasts: <code>podcasts.apple.com/us/podcast/name/id123456789</code><br>RSS feed: any direct RSS/XML URL</div>';
  h += '<div class="status" id="impStatus"></div>';
  h += '<div class="preview" id="impPreview"></div>';
  h += '<button class="bg" id="impBtn" onclick="doImport()">Fetch &amp; Download CSV</button>';
  h += '</div>';

  h += '<footer>Eclipse Podcast Addon v2.0.0 &bull; Powered by Podcast Index &bull; <a href="' + baseUrl + '/health" target="_blank" style="color:#333;text-decoration:none">' + baseUrl + '</a></footer>';

  // ── Client JS ─────────────────────────────────────────────────────────────
  h += '<script>';
  h += 'var _gu="",_ru="";';

  // credential change — enable/disable generate button
  h += 'function onCredChange(){';
  h += 'var k=document.getElementById("piKey").value.trim(),s=document.getElementById("piSecret").value.trim();';
  h += 'var btn=document.getElementById("genBtn");';
  h += 'if(k&&s){btn.disabled=false;btn.textContent="Generate My Addon URL";}';
  h += 'else{btn.disabled=true;btn.textContent="Enter your API credentials first";}';
  h += '}';

  // generate
  h += 'function generate(){';
  h += 'var k=document.getElementById("piKey").value.trim(),s=document.getElementById("piSecret").value.trim();';
  h += 'if(!k||!s){alert("Enter your Podcast Index API Key and Secret first.");return;}';
  h += 'var btn=document.getElementById("genBtn"),st=document.getElementById("credStatus");';
  h += 'btn.disabled=true;btn.textContent="Validating credentials...";st.className="status spin";st.textContent="Checking your API key with Podcast Index...";';
  h += 'fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({piKey:k,piSecret:s})})';
  h += '.then(function(r){return r.json();})';
  h += '.then(function(d){';
  h += 'if(d.error){st.className="status err";st.textContent=d.error;btn.disabled=false;btn.textContent="Generate My Addon URL";return;}';
  h += '_gu=d.manifestUrl;';
  h += 'document.getElementById("genUrl").textContent=_gu;';
  h += 'document.getElementById("genBox").style.display="block";';
  h += 'document.getElementById("impToken").value=_gu;';
  h += 'st.className="status ok";st.textContent="\u2713 Credentials valid \u2014 your addon URL is ready";';
  h += 'btn.disabled=false;btn.textContent="Regenerate URL";';
  h += '})';
  h += '.catch(function(e){st.className="status err";st.textContent="Error: "+e.message;btn.disabled=false;btn.textContent="Generate My Addon URL";});';
  h += '}';

  // copy gen
  h += 'function copyGen(){if(!_gu)return;navigator.clipboard.writeText(_gu).then(function(){var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  // refresh
  h += 'function doRefresh(){';
  h += 'var btn=document.getElementById("refBtn"),eu=document.getElementById("existingUrl").value.trim();';
  h += 'if(!eu){alert("Paste your existing addon URL first.");return;}';
  h += 'btn.disabled=true;btn.textContent="Checking...";';
  h += 'fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})})';
  h += '.then(function(r){return r.json();})';
  h += '.then(function(d){';
  h += 'if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Restore Existing URL";return;}';
  h += '_ru=d.manifestUrl;document.getElementById("refUrl").textContent=_ru;document.getElementById("refBox").style.display="block";document.getElementById("impToken").value=_ru;';
  h += 'btn.disabled=false;btn.textContent="Restore Again";';
  h += '})';
  h += '.catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Restore Existing URL";});';
  h += '}';

  h += 'function copyRef(){if(!_ru)return;navigator.clipboard.writeText(_ru).then(function(){var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  // importer helpers
  h += 'function getTok(s){var m=s.match(/\\/u\\/([a-f0-9]{28})\\//);return m?m[1]:null;}';
  h += 'function hesc(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}';

  // import
  h += 'function doImport(){';
  h += 'var btn=document.getElementById("impBtn"),raw=document.getElementById("impToken").value.trim(),purl=document.getElementById("impUrl").value.trim(),st=document.getElementById("impStatus"),pv=document.getElementById("impPreview");';
  h += 'if(!raw){st.className="status err";st.textContent="Paste your addon URL first.";return;}';
  h += 'if(!purl){st.className="status err";st.textContent="Paste a podcast URL.";return;}';
  h += 'var tok=getTok(raw);if(!tok){st.className="status err";st.textContent="Could not find your token in the URL.";return;}';
  h += 'btn.disabled=true;btn.textContent="Fetching...";st.className="status spin";st.textContent="Fetching episodes...";pv.style.display="none";';
  h += 'fetch("/u/"+tok+"/import?url="+encodeURIComponent(purl))';
  h += '.then(function(r){if(!r.ok)return r.json().then(function(e){throw new Error(e.error||"Server error "+r.status);});return r.json();})';
  h += '.then(function(data){';
  h += 'var tracks=data.tracks||[];if(!tracks.length)throw new Error("No episodes found.");';
  h += 'var rows=tracks.slice(0,50).map(function(t,i){return\'<div class="tr"><span class="tn">\'+(i+1)+\'</span><div class="ti"><div class="tt">\'+hesc(t.title)+\'</div><div class="ta">\'+hesc(t.artist)+\'</div></div></div>\';}).join("");';
  h += 'if(tracks.length>50)rows+=\'<div class="tr" style="text-align:center;color:#555">+\'+(tracks.length-50)+\' more</div>\';';
  h += 'pv.innerHTML=rows;pv.style.display="block";';
  h += 'st.className="status ok";st.textContent="Found "+tracks.length+" episodes in \\""+data.title+"\\"";';
  h += 'var lines=["Title,Artist,Album,Duration"];';
  h += 'tracks.forEach(function(t){function ce(s){s=String(s||"");if(s.indexOf(",")!==-1||s.indexOf("\\"")!==-1){s=\'"\'+s.replace(/"/g,\'""\')+\'"\'}return s;}lines.push(ce(t.title)+","+ce(t.artist)+","+ce(data.title||"")+","+ce(t.duration||""));});';
  h += 'var blob=new Blob([lines.join("\\n")],{type:"text/csv"});var a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=(data.title||"podcast").replace(/[^a-zA-Z0-9 _-]/g,"").trim()+".csv";document.body.appendChild(a);a.click();document.body.removeChild(a);';
  h += 'btn.disabled=false;btn.textContent="Fetch & Download CSV";';
  h += '})';
  h += '.catch(function(e){st.className="status err";st.textContent=e.message;btn.disabled=false;btn.textContent="Fetch & Download CSV";});';
  h += '}';
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

  if (!piKey || !piSec) return res.status(400).json({ error: 'Podcast Index API Key and Secret are required.' });

  // Validate credentials against the real PI API before issuing a token
  const valid = await piValidate(piKey, piSec);
  if (!valid) return res.status(401).json({ error: 'Invalid Podcast Index credentials. Check your API Key and Secret at api.podcastindex.org.' });

  const bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens today from this IP.' });

  const token = generateToken();
  const entry = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [], piKey, piSecret: piSec };
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
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste your full addon URL.' });
  const entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate a new one.' });
  res.json({ token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

// ─── Manifest ─────────────────────────────────────────────────────────────
app.get('/u/:token/manifest.json', tokenMiddleware, function (req, res) {
  res.json({
    id:          'com.eclipse.podcasts.' + req.params.token.slice(0, 8),
    name:        'Podcasts',
    version:     '2.0.0',
    description: 'Search and stream any podcast. 4M+ shows via Podcast Index.',
    icon:        'https://is1-ssl.mzstatic.com/image/thumb/Purple126/v4/32/dc/fc/32dcfc3a-d9aa-46b3-85fb-2bc8a9d3a574/AppIcon-0-0-1x_U007emarketing-0-10-0-85-220.png/512x512bb.jpg',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  });
});

// ─── Search ───────────────────────────────────────────────────────────────
app.get('/u/:token/search', tokenMiddleware, async function (req, res) {
  const q     = cleanText(req.query.q);
  const entry = req.tokenEntry;
  if (!q) return res.json({ tracks: [], albums: [], artists: [], playlists: [] });

  try {
    // Run PI show search + iTunes episode search in parallel
    const [piData, itunesData] = await Promise.all([
      piGet(entry, '/search/byterm', { q, max: 15 }),
      itunesGet('/search', { term: q, media: 'podcast', entity: 'podcastEpisode', limit: 20, explicit: 'Yes' })
    ]);

    const feeds = (piData && piData.feeds) || [];

    // ── Tracks (episodes) from iTunes — better episode-level search than PI ──
    const tracks = ((itunesData && itunesData.results) || [])
      .filter(ep => ep.kind === 'podcast-episode' && ep.episodeUrl)
      .map(mapItunesEpisode);

    // ── Albums (shows) from PI ────────────────────────────────────────────
    const albums = feeds.map(mapPiFeed);

    // ── Artists derived from PI feeds (dedup by author) ───────────────────
    const artistMap = new Map();
    feeds.forEach(f => {
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

    // ── Playlists — each PI feed with a URL becomes a browsable RSS playlist ─
    const playlists = feeds
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

// ─── Stream ───────────────────────────────────────────────────────────────
app.get('/u/:token/stream/:id', tokenMiddleware, async function (req, res) {
  const id    = req.params.id;
  const entry = req.tokenEntry;

  // Check episode cache first (covers pi_ep_, rss_ep_, ep_ after first load)
  const cached = EPISODE_CACHE.get(id);
  if (cached && cached.streamURL) return res.json({ url: cached.streamURL, format: cached.format || 'mp3' });

  // PI episode fallback (server restarted, not in cache)
  if (id.startsWith('pi_ep_')) {
    const epId = id.replace('pi_ep_', '');
    const data = await piGet(entry, '/episodes/byid', { id: epId });
    if (data && data.episode && data.episode.enclosureUrl) {
      const ep = mapPiEpisode(data.episode, '');
      return res.json({ url: ep.streamURL, format: ep.format });
    }
  }

  // iTunes episode fallback
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

// ─── Album (= Podcast show via PI) ────────────────────────────────────────
app.get('/u/:token/album/:id', tokenMiddleware, async function (req, res) {
  const rawId  = req.params.id;
  const entry  = req.tokenEntry;
  const feedId = rawId.replace('pi_', '');

  try {
    const [feedData, epData] = await Promise.all([
      piGet(entry, '/podcasts/byfeedid', { id: feedId }),
      piGet(entry, '/episodes/byfeedid', { id: feedId, max: 200, fulltext: true })
    ]);

    const feed   = (feedData && feedData.feed) || {};
    const items  = (epData   && epData.items)  || [];
    const tracks = items.map(ep => mapPiEpisode(ep, feed.title || ''));

    res.json({
      id:          rawId,
      title:       cleanText(feed.title || ''),
      artist:      cleanText(feed.author || feed.ownerName || ''),
      artworkURL:  feed.image || feed.artwork || null,
      year:        feed.newestItemPublishTime ? String(new Date(feed.newestItemPublishTime * 1000).getFullYear()) : null,
      description: cleanText(feed.description || '').slice(0, 500),
      trackCount:  tracks.length,
      tracks
    });
  } catch (e) {
    console.error('[album]', e.message);
    res.status(500).json({ error: 'Show fetch failed.' });
  }
});

// ─── Artist (= Podcast creator via PI) ───────────────────────────────────
app.get('/u/:token/artist/:id', tokenMiddleware, async function (req, res) {
  const rawId = req.params.id;
  const entry = req.tokenEntry;

  try {
    const authorName = Buffer.from(rawId.replace('pi_author_', ''), 'base64url').toString('utf8');
    const data   = await piGet(entry, '/search/byterm', { q: authorName, max: 20 });
    const feeds  = ((data && data.feeds) || []);

    // Prefer exact author matches, fall back to all results
    const exact   = feeds.filter(f => cleanText(f.author || '').toLowerCase() === authorName.toLowerCase());
    const matched = exact.length > 0 ? exact : feeds.slice(0, 10);
    const first   = matched[0] || {};

    // Fetch recent episodes from the creator's first show as topTracks
    let topTracks = [];
    if (first.id) {
      try {
        const epData  = await piGet(entry, '/episodes/byfeedid', { id: first.id, max: 10 });
        topTracks = ((epData && epData.items) || []).map(ep => mapPiEpisode(ep, first.title || ''));
      } catch (e2) { /* skip */ }
    }

    res.json({
      id:         rawId,
      name:       authorName,
      artworkURL: first.image || first.artwork || null,
      bio:        null,
      genres:     first.categories ? Object.values(first.categories).slice(0, 2) : [],
      topTracks,
      albums:     matched.map(mapPiFeed)
    });
  } catch (e) {
    console.error('[artist]', e.message);
    res.status(500).json({ error: 'Artist fetch failed.' });
  }
});

// ─── Playlist (= RSS feed) ────────────────────────────────────────────────
app.get('/u/:token/playlist/:id', tokenMiddleware, async function (req, res) {
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

// ─── Import ───────────────────────────────────────────────────────────────
app.get('/u/:token/import', tokenMiddleware, async function (req, res) {
  const inputUrl = cleanText(req.query.url);
  if (!inputUrl) return res.status(400).json({ error: 'Pass ?url= with an Apple Podcasts or RSS feed URL.' });

  // Apple Podcasts URL
  const appleMatch = inputUrl.match(/\/id(\d{6,12})/);
  if (appleMatch) {
    const podId = appleMatch[1];
    const data  = await itunesGet('/lookup', { id: podId, entity: 'podcastEpisode', limit: 300 });
    if (!data || !data.results || data.results.length === 0) return res.status(404).json({ error: 'Podcast not found on Apple Podcasts.' });
    const show     = data.results.find(r => r.wrapperType === 'collection') || data.results[0];
    const episodes = data.results.filter(r => r.kind === 'podcast-episode' && r.episodeUrl);
    return res.json({ title: cleanText(show.collectionName || show.trackName || 'Podcast'), artworkURL: artworkHd(show.artworkUrl600), tracks: episodes.map(mapItunesEpisode) });
  }

  // RSS feed URL
  if (/^https?:\/\//i.test(inputUrl)) {
    const xml = await fetchRss(inputUrl);
    if (!xml) return res.status(404).json({ error: 'Could not fetch RSS feed.' });
    const parsed = parseRssToShow(xml);
    if (!parsed) return res.status(500).json({ error: 'Could not parse the RSS feed.' });
    return res.json({ title: parsed.title, artworkURL: parsed.artworkURL, tracks: parsed.tracks });
  }

  return res.status(400).json({ error: 'Use an Apple Podcasts URL (podcasts.apple.com/.../idXXXX) or a direct RSS feed URL.' });
});

// ─── Health ───────────────────────────────────────────────────────────────
app.get('/health', function (req, res) {
  res.json({ status: 'ok', version: '2.0.0', redisConnected: !!(redis && redis.status === 'ready'), activeTokens: TOKEN_CACHE.size, cachedEpisodes: EPISODE_CACHE.size, cachedFeeds: FEED_URL_CACHE.size, timestamp: new Date().toISOString() });
});

app.listen(PORT, () => console.log('Eclipse Podcast Addon v2.0.0 on port ' + PORT));
