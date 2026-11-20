/**
 * YouTube Proxy — Express server for Fly.io
 */

import express from "express";
import { pipeline } from "stream/promises";
import { Readable } from "stream";
import { spawn } from "child_process";

const app = express();
const PORT = process.env.PORT || 8080;

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
  const target = req.query.url;
  if (!target) return res.status(400).json({ error: "Missing url param" });

  let decoded;
  try {
    decoded = decodeURIComponent(target);
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
// GET /stream?v=VIDEO_ID&q=720p  — mux video+audio with ffmpeg, pipe as mp4
// ---------------------------------------------------------------------------
app.get("/stream", async (req, res) => {
  const videoId = req.query.v;
  const quality = req.query.q || "720p";
  if (!videoId || !/^[a-zA-Z0-9_-]{11}$/.test(videoId)) {
    return res.status(400).json({ error: "Missing or invalid video ID" });
  }

  // Audio itags: 140 = m4a 128k (best for mp4 container), 251 = opus webm
  const AUDIO_ITAGS = ["140", "251", "250", "249"];
  const VIDEO_ITAGS = {
    "1080p": ["137", "248", "299", "303"],
    "720p":  ["136", "247", "298", "302"],
    "360p":  ["134", "243", "18"],
  };

  let playerData;
  try {
    playerData = await fetchPlayerData(videoId);
  } catch (err) {
    return res.status(502).json({ error: err.message });
  }

  const all = [
    ...(playerData?.streamingData?.formats || []),
    ...(playerData?.streamingData?.adaptiveFormats || []),
  ];

  const itags = VIDEO_ITAGS[quality] || VIDEO_ITAGS["720p"];
  const videoStream = all.find(f => itags.includes(String(f.itag)) && f.url);
  const audioStream = all.find(f => AUDIO_ITAGS.includes(String(f.itag)) && f.url);

  if (!videoStream) return res.status(404).json({ error: `No ${quality} stream found` });
  if (!audioStream) return res.status(404).json({ error: "No audio stream found" });

  // Remove Transfer-Encoding — Node handles chunked automatically for piped responses
  res.setHeader("Content-Type", "video/mp4");
  res.setHeader("X-Content-Type-Options", "nosniff");
  Object.entries(CORS_HEADERS).forEach(([k, v]) => res.setHeader(k, v));

  // ffmpeg: copy both streams (no re-encoding = fast first byte), frag_keyframe for streaming
  // -c:a copy works because itag 140 is already AAC in an m4a container
  // drop 'faststart' — it only applies to files, not pipe output
  const ff = spawn("ffmpeg", [
    // Minimize startup buffering — output first fragment ASAP
    "-fflags", "+nobuffer+genpts",
    "-probesize", "32",
    "-analyzeduration", "0",
    "-user_agent", "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip",
    "-i", videoStream.url,
    "-user_agent", "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip",
    "-i", audioStream.url,
    "-c:v", "copy",
    "-c:a", "copy",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof",
    "-frag_duration", "1000000",   // 1-second fragments = browser gets data quickly
    "-f", "mp4",
    "pipe:1",
  ], { stdio: ["ignore", "pipe", "pipe"] });

  ff.stdout.pipe(res);

  // Log ffmpeg stderr for debugging
  let ffmpegLog = "";
  ff.stderr.on("data", (d) => {
    ffmpegLog += d.toString();
  });

  ff.on("close", (code) => {
    if (code !== 0) console.error(`ffmpeg exited ${code}:\n${ffmpegLog.slice(-1000)}`);
  });

  req.on("close", () => ff.kill("SIGKILL"));
  ff.on("error", (err) => {
    console.error("ffmpeg spawn error:", err.message);
    if (!res.headersSent) res.status(500).json({ error: "ffmpeg failed to start" });
  });
  ff.on("exit", (code) => {
    if (code !== 0 && code !== null) console.warn("ffmpeg exited with code", code);
  });
});

// ---------------------------------------------------------------------------
// Fetch YouTube player data
// ---------------------------------------------------------------------------
async function fetchPlayerData(videoId) {
  const API_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8";
  const endpoint = `https://www.youtube.com/youtubei/v1/player?key=${API_KEY}&prettyPrint=false`;

  const body = {
    videoId,
    contentCheckOk: true,
    racyCheckOk: true,
    context: {
      client: {
        clientName: "ANDROID",
        clientVersion: "20.10.38",
        androidSdkVersion: 34,
        hl: "en",
        gl: "US",
      },
    },
  };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip",
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
