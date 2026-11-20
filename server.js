/**
 * YouTube Proxy — Express server for Fly.io
 */

import express from "express";
import { pipeline } from "stream/promises";
import { Readable } from "stream";
import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";

const app = express();
const PORT = process.env.PORT || 8080;

// Cache dir for muxed files + in-flight mux tracking
const CACHE_DIR = path.join(os.tmpdir(), "yt-mux");
fs.mkdirSync(CACHE_DIR, { recursive: true });
const inFlight = new Map(); // key -> Promise<string filePath>

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Range, Content-Type",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
};

const QUALITY_ITAGS = {
  "1080p": ["137", "248", "299", "303"],
  "720p":  ["136", "247", "298", "302"],
  "360p":  ["134", "243", "18"],
};

// Apply CORS to every response
app.use((req, res, next) => {
  Object.entries(CORS_HEADERS).forEach(([k, v]) => res.setHeader(k, v));
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ---------------------------------------------------------------------------
// GET /info?v=VIDEO_ID
// ---------------------------------------------------------------------------
app.get("/info", async (req, res) => {
  const videoId = req.query.v;
  if (!videoId || !/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
    return res.status(400).json({ error: "Missing or invalid video ID" });
  }
  try {
    const playerData = await fetchPlayerData(videoId);
    const streams = extractStreams(playerData, req);
    const details = playerData?.videoDetails || {};
    res.json({
      id: videoId,
      title: details.title || "Unknown",
      author: details.author || "Unknown",
      duration: parseInt(details.lengthSeconds || "0"),
      thumbnail: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
      streams,
    });
  } catch (err) {
    res.status(502).json({ error: `Failed to fetch video info: ${err.message}` });
  }
});

// ---------------------------------------------------------------------------
// GET /formats?v=VIDEO_ID
// ---------------------------------------------------------------------------
app.get("/formats", async (req, res) => {
  const videoId = req.query.v;
  if (!videoId) return res.status(400).json({ error: "Missing video ID" });
  try {
    const playerData = await fetchPlayerData(videoId);
    const formats = [
      ...(playerData?.streamingData?.formats || []),
      ...(playerData?.streamingData?.adaptiveFormats || []),
    ].map(f => ({
      itag: f.itag,
      mimeType: f.mimeType,
      quality: f.quality,
      qualityLabel: f.qualityLabel,
      hasUrl: !!f.url,
    }));
    res.json({ formats });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /raw?v=VIDEO_ID — debug
// ---------------------------------------------------------------------------
app.get("/raw", async (req, res) => {
  const videoId = req.query.v;
  if (!videoId) return res.status(400).json({ error: "Missing video ID" });
  try {
    const playerData = await fetchPlayerData(videoId);
    res.json({
      playabilityStatus: playerData?.playabilityStatus,
      hasStreamingData: !!playerData?.streamingData,
      hasVideoDetails: !!playerData?.videoDetails,
      streamingDataKeys: playerData?.streamingData ? Object.keys(playerData.streamingData) : [],
      videoDetailsKeys: playerData?.videoDetails ? Object.keys(playerData.videoDetails) : [],
      formatsCount: (playerData?.streamingData?.formats || []).length,
      adaptiveFormatsCount: (playerData?.streamingData?.adaptiveFormats || []).length,
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /proxy?url=ENCODED_URL — streams video bytes
// ---------------------------------------------------------------------------
app.get("/proxy", async (req, res) => {
  // Express already percent-decodes query params once. Do NOT decode again —
  // a second decodeURIComponent corrupts the YouTube signature (%2C→',', %3D→'=')
  // which makes googlevideo return 403.
  const decoded = req.query.url;
  if (!decoded) return res.status(400).json({ error: "Missing url param" });

  try {
    new URL(decoded);
  } catch {
    return res.status(400).json({ error: "Invalid URL" });
  }

  const allowed = ["googlevideo.com", "youtube.com", "ytimg.com"];
  const targetHost = new URL(decoded).hostname;
  if (!allowed.some(d => targetHost.endsWith(d))) {
    return res.status(403).json({ error: "Domain not allowed" });
  }

  const headers = {
    "User-Agent": "Mozilla/5.0 (compatible; FlyProxy/1.0)",
    "Referer": "https://www.youtube.com/",
    "Origin": "https://www.youtube.com",
  };
  if (req.headers["range"]) headers["Range"] = req.headers["range"];

  try {
    const upstream = await fetch(decoded, { headers, redirect: "follow" });

    res.status(upstream.status);
    res.setHeader("Content-Type", upstream.headers.get("Content-Type") || "video/mp4");
    res.setHeader("Cache-Control", "public, max-age=3600");
    const cl = upstream.headers.get("Content-Length");
    const cr = upstream.headers.get("Content-Range");
    const ar = upstream.headers.get("Accept-Ranges");
    if (cl) res.setHeader("Content-Length", cl);
    if (cr) res.setHeader("Content-Range", cr);
    if (ar) res.setHeader("Accept-Ranges", ar);

    await pipeline(Readable.fromWeb(upstream.body), res);
  } catch (err) {
    if (!res.headersSent) res.status(502).json({ error: `Upstream fetch failed: ${err.message}` });
  }
});

// ---------------------------------------------------------------------------
// Mux video+audio to a cached temp file (returns path). iOS/iPad Safari will
// NOT play a live pipe — it needs a seekable file with a real Content-Length
// and byte-range support. So we mux to disk once, cache it, then range-serve.
// ---------------------------------------------------------------------------
const AUDIO_ITAGS = ["140", "251", "250", "249"];
const VIDEO_ITAGS_MUX = {
  "1080p": ["137", "248", "299", "303"],
  "720p":  ["136", "247", "298", "302"],
  "360p":  ["134", "243", "18"],
};

const STREAM_FETCH_HEADERS = {
  "User-Agent": "Mozilla/5.0 (compatible; FlyProxy/1.0)",
  "Referer": "https://www.youtube.com/",
  "Origin": "https://www.youtube.com",
};

// Download a YouTube stream URL to a local file, streamed to disk (no big RAM use).
// No Range header (plain GET returned 200 in the probe) and no extra options that
// might trip abuse detection.
async function downloadStream(url, destPath) {
  const res = await fetch(url, { headers: STREAM_FETCH_HEADERS, redirect: "follow" });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`stream download ${res.status}: ${body.slice(0, 160)}`);
  }
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(destPath));
}

async function muxToFile(videoId, quality) {
  const key = `${videoId}_${quality}`;
  const filePath = path.join(CACHE_DIR, `${key}.mp4`);

  // Already cached and non-empty → reuse
  if (fs.existsSync(filePath) && fs.statSync(filePath).size > 0) return filePath;
  // Already being muxed → wait for that one
  if (inFlight.has(key)) return inFlight.get(key);

  const job = (async () => {
    const playerData = await fetchPlayerData(videoId);
    const all = [
      ...(playerData?.streamingData?.formats || []),
      ...(playerData?.streamingData?.adaptiveFormats || []),
    ];
    const itags = VIDEO_ITAGS_MUX[quality] || VIDEO_ITAGS_MUX["720p"];
    const videoStream = all.find(f => itags.includes(String(f.itag)) && f.url);
    const audioStream = all.find(f => AUDIO_ITAGS.includes(String(f.itag)) && f.url);
    if (!videoStream) throw new Error(`No ${quality} video stream found`);
    if (!audioStream) throw new Error("No audio stream found");

    const tmpPath = filePath + ".part";
    const videoTmp = path.join(CACHE_DIR, `${key}.v.tmp`);
    const audioTmp = path.join(CACHE_DIR, `${key}.a.tmp`);

    // Download both streams to disk with node fetch (which works — proven by the
    // probe). ffmpeg hitting googlevideo URLs directly gets 403s due to its HTTP
    // signature/header handling, so we never let ffmpeg touch the network.
    try {
      // Sequential, not parallel — two simultaneous googlevideo connections from
      // the same IP can trip abuse detection and 403.
      await downloadStream(videoStream.url, videoTmp);
      await downloadStream(audioStream.url, audioTmp);

      // Mux the two local files → seekable mp4 (+faststart for Safari).
      await new Promise((resolve, reject) => {
        const ff = spawn("ffmpeg", [
          "-y",
          "-i", videoTmp,
          "-i", audioTmp,
          "-map", "0:v:0", "-map", "1:a:0",
          "-c:v", "copy",
          "-c:a", "aac", "-b:a", "128k",
          "-movflags", "+faststart",
          "-f", "mp4",
          tmpPath,
        ], { stdio: ["ignore", "ignore", "pipe"] });

        let log = "";
        ff.stderr.on("data", d => { log += d.toString(); });
        ff.on("error", reject);
        ff.on("close", code => {
          if (code === 0) resolve();
          else reject(new Error(`ffmpeg exited ${code}: ${log.slice(-500)}`));
        });
      });

      fs.renameSync(tmpPath, filePath);
      return filePath;
    } finally {
      // Clean up the raw input files regardless of success
      for (const f of [videoTmp, audioTmp]) {
        try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch {}
      }
    }
  })();

  inFlight.set(key, job);
  try {
    return await job;
  } finally {
    inFlight.delete(key);
  }
}

// ---------------------------------------------------------------------------
// GET /stream?v=VIDEO_ID&q=720p — mux to cached file, serve with range support
// ---------------------------------------------------------------------------
app.get("/stream", async (req, res) => {
  const videoId = req.query.v;
  const quality = req.query.q || "720p";
  if (!videoId || !/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
    return res.status(400).json({ error: "Missing or invalid video ID" });
  }

  let filePath;
  try {
    filePath = await muxToFile(videoId, quality);
  } catch (err) {
    console.error("mux error:", err.message);
    if (!res.headersSent) return res.status(502).json({ error: err.message });
    return;
  }

  const stat = fs.statSync(filePath);
  const total = stat.size;

  Object.entries(CORS_HEADERS).forEach(([k, v]) => res.setHeader(k, v));
  res.setHeader("Content-Type", "video/mp4");
  res.setHeader("Accept-Ranges", "bytes");
  res.setHeader("Cache-Control", "public, max-age=3600");

  const range = req.headers["range"];
  if (range) {
    // Parse "bytes=START-END"
    const m = /bytes=(\d*)-(\d*)/.exec(range);
    let start = m && m[1] ? parseInt(m[1], 10) : 0;
    let end = m && m[2] ? parseInt(m[2], 10) : total - 1;
    if (isNaN(start) || start < 0) start = 0;
    if (isNaN(end) || end >= total) end = total - 1;
    if (start > end) { start = 0; end = total - 1; }

    res.status(206);
    res.setHeader("Content-Range", `bytes ${start}-${end}/${total}`);
    res.setHeader("Content-Length", end - start + 1);
    fs.createReadStream(filePath, { start, end }).pipe(res);
  } else {
    res.status(200);
    res.setHeader("Content-Length", total);
    fs.createReadStream(filePath).pipe(res);
  }
});

// ---------------------------------------------------------------------------
// GET /prepare?v=VIDEO_ID&q=720p — kick off mux, report readiness (for polling)
// ---------------------------------------------------------------------------
const muxErrors = new Map(); // key -> error message

app.get("/prepare", (req, res) => {
  const videoId = req.query.v;
  const quality = req.query.q || "720p";
  if (!videoId || !/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
    return res.status(400).json({ error: "Missing or invalid video ID" });
  }
  Object.entries(CORS_HEADERS).forEach(([k, v]) => res.setHeader(k, v));

  const key = `${videoId}_${quality}`;
  const filePath = path.join(CACHE_DIR, `${key}.mp4`);

  if (fs.existsSync(filePath) && fs.statSync(filePath).size > 0) {
    return res.json({ ready: true });
  }
  if (muxErrors.has(key)) {
    const msg = muxErrors.get(key);
    muxErrors.delete(key);
    return res.status(502).json({ ready: false, error: msg });
  }
  // Start mux if not already running (fire-and-forget; dedup via inFlight)
  if (!inFlight.has(key)) {
    muxToFile(videoId, quality).catch(e => {
      console.error("prepare mux:", e.message);
      muxErrors.set(key, e.message);
    });
  }
  res.json({ ready: false });
});

// ---------------------------------------------------------------------------
// GET /streamdebug?v=VIDEO_ID&q=720p — run ffmpeg, return stderr instead of video
// ---------------------------------------------------------------------------
app.get("/streamdebug", async (req, res) => {
  const videoId = req.query.v;
  const quality = req.query.q || "720p";
  if (!videoId) return res.status(400).json({ error: "Missing video ID" });

  const AUDIO_ITAGS = ["140", "251", "250", "249"];
  const VIDEO_ITAGS = {
    "1080p": ["137", "248", "299", "303"],
    "720p":  ["136", "247", "298", "302"],
    "360p":  ["134", "243", "18"],
  };

  let playerData;
  try { playerData = await fetchPlayerData(videoId); }
  catch (err) { return res.status(502).json({ error: err.message }); }

  const all = [...(playerData?.streamingData?.formats || []), ...(playerData?.streamingData?.adaptiveFormats || [])];
  const videoStream = all.find(f => (VIDEO_ITAGS[quality] || VIDEO_ITAGS["720p"]).includes(String(f.itag)) && f.url);
  const audioStream = all.find(f => AUDIO_ITAGS.includes(String(f.itag)) && f.url);

  if (!videoStream) return res.status(404).json({ error: `No ${quality} stream`, available: all.map(f => ({itag: f.itag, q: f.qualityLabel, mime: f.mimeType})) });
  if (!audioStream) return res.status(404).json({ error: "No audio stream" });

  // Probe the stream URLs with different UA / Range combos to find what YouTube accepts.
  const ANDROID_UA = "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip";
  const CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
  const combos = [
    { name: "generic UA, no range",   ua: "Mozilla/5.0 (compatible; FlyProxy/1.0)", range: null },
    { name: "generic UA, range 0-",   ua: "Mozilla/5.0 (compatible; FlyProxy/1.0)", range: "bytes=0-" },
    { name: "chrome UA, range 0-",    ua: CHROME_UA, range: "bytes=0-" },
    { name: "android UA, no range",   ua: ANDROID_UA, range: null },
    { name: "android UA, range 0-",   ua: ANDROID_UA, range: "bytes=0-" },
  ];

  async function probe(url) {
    const out = [];
    for (const c of combos) {
      const headers = { "User-Agent": c.ua, "Referer": "https://www.youtube.com/", "Origin": "https://www.youtube.com" };
      if (c.range) headers["Range"] = c.range;
      try {
        const r = await fetch(url, { headers, redirect: "follow" });
        let snippet = "";
        if (!r.ok) snippet = (await r.text().catch(() => "")).slice(0, 120).replace(/\s+/g, " ");
        out.push(`  [${r.status}] ${c.name}${snippet ? " — " + snippet : ""}`);
        // Drain body on success to free the connection
        if (r.ok && r.body) r.body.cancel?.();
      } catch (e) {
        out.push(`  [ERR] ${c.name} — ${e.message}`);
      }
    }
    return out.join("\n");
  }

  // Real download test — exactly what muxToFile does, to see if a full download
  // diverges from the lightweight probe.
  async function realDownloadTest(url) {
    try {
      const r = await fetch(url, { headers: STREAM_FETCH_HEADERS, redirect: "follow" });
      if (!r.ok) {
        const body = (await r.text().catch(() => "")).slice(0, 160).replace(/\s+/g, " ");
        return `  [${r.status}] full download FAILED — ${body}`;
      }
      // Read a few chunks to confirm bytes actually flow
      let bytes = 0;
      const reader = r.body.getReader();
      while (bytes < 1_000_000) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += value.length;
      }
      reader.cancel?.();
      return `  [${r.status}] full download OK — read ${bytes} bytes`;
    } catch (e) {
      return `  [ERR] full download — ${e.message}`;
    }
  }

  res.setHeader("Content-Type", "text/plain");
  const lines = [];
  lines.push(`video itag=${videoStream.itag} mime=${videoStream.mimeType}`);
  lines.push(`audio itag=${audioStream.itag} mime=${audioStream.mimeType}`);
  lines.push(`playabilityStatus=${playerData?.playabilityStatus?.status}`);
  lines.push("");
  lines.push("VIDEO stream probes:");
  lines.push(await probe(videoStream.url));
  lines.push("");
  lines.push("VIDEO real download:");
  lines.push(await realDownloadTest(videoStream.url));
  lines.push("");
  lines.push("AUDIO stream probes:");
  lines.push(await probe(audioStream.url));
  lines.push("");
  lines.push("AUDIO real download:");
  lines.push(await realDownloadTest(audioStream.url));
  res.send(lines.join("\n"));
});

// ---------------------------------------------------------------------------
// Fetch YouTube player data
// ---------------------------------------------------------------------------
async function fetchPlayerData(videoId) {
  const API_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8";
  const endpoint = `https://www.youtube.com/youtubei/v1/player?key=${API_KEY}&prettyPrint=false`;

  // "android_sdkless" client — the ANDROID client WITHOUT androidSdkVersion.
  // The androidSdkVersion field is what triggers YouTube's PO Token requirement,
  // which makes adaptive (720p/1080p/audio) stream URLs return 403 while the
  // progressive 360p (itag 18) still works. Dropping it = no PO token needed.
  // Config copied from yt-dlp's _base.py android_sdkless client.
  const ANDROID_SDKLESS_UA = "com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip";
  const body = {
    videoId,
    contentCheckOk: true,
    racyCheckOk: true,
    context: {
      client: {
        clientName: "ANDROID",
        clientVersion: "20.10.38",
        // NO androidSdkVersion — intentionally omitted
        userAgent: ANDROID_SDKLESS_UA,
        hl: "en",
        gl: "US",
      },
    },
  };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": ANDROID_SDKLESS_UA,
      "X-YouTube-Client-Name": "3",
      "X-YouTube-Client-Version": "20.10.38",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`YouTube API returned ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Extract stream URLs
// ---------------------------------------------------------------------------
function extractStreams(playerData, req) {
  const base = `${req.protocol}://${req.get("host")}`;
  const all = [
    ...(playerData?.streamingData?.formats || []),
    ...(playerData?.streamingData?.adaptiveFormats || []),
  ];
  const streams = {};
  for (const [quality, itags] of Object.entries(QUALITY_ITAGS)) {
    const match = all.find(f => itags.includes(String(f.itag)) && f.url);
    if (match) {
      streams[quality] = {
        proxyUrl: `${base}/proxy?url=${encodeURIComponent(match.url)}`,
        directUrl: match.url,
        mimeType: match.mimeType || "video/mp4",
        itag: match.itag,
        qualityLabel: match.qualityLabel || quality,
      };
    }
  }
  return streams;
}

app.listen(PORT, () => console.log(`Listening on :${PORT}`));
