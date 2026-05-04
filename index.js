import admin from "firebase-admin";
import Parser from "rss-parser";
import crypto from "crypto";
import https from "https";

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const parser = new Parser({
  timeout: 10000,
  headers: { "User-Agent": "RSSAggregator/1.0" },
  customFields: {
    item: [
      ["media:content", "mediaContent"],
      ["media:thumbnail", "mediaThumbnail"],
      ["enclosure", "enclosure"],
      ["category", "categories"],
    ],
  },
});

const MAX_ARTICLES_PER_SOURCE = 5;

function hashLink(link) {
  return crypto.createHash("md5").update(link).digest("hex");
}

function toTimestamp(dateStr) {
  if (!dateStr) return admin.firestore.Timestamp.now();
  const d = new Date(dateStr);
  return isNaN(d) ? admin.firestore.Timestamp.now() : admin.firestore.Timestamp.fromDate(d);
}

function slugify(text) {
  return text
    .toString()
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "-")
    .replace(/[^\w\u0600-\u06FF-]+/g, "")
    .replace(/--+/g, "-")
    .slice(0, 100);
}

function encodeRssUrl(rawUrl) {
  try {
    new URL(rawUrl);
    if (!/[^\x00-\x7F]/.test(rawUrl)) return rawUrl;
    const url = new URL(rawUrl);
    url.pathname = url.pathname.split("/").map(segment => encodeURIComponent(decodeURIComponent(segment))).join("/");
    url.search = url.search ? "?" + url.search.slice(1).split("&").map(p => {
      const [k, v] = p.split("=");
      return `${encodeURIComponent(decodeURIComponent(k || ""))}=${encodeURIComponent(decodeURIComponent(v || ""))}`;
    }).join("&") : "";
    return url.toString();
  } catch {
    return encodeURI(rawUrl);
  }
}

function extractImageUrl(item) {
  if (item.mediaContent?.$?.url) return item.mediaContent.$.url;
  if (item.mediaThumbnail?.$?.url) return item.mediaThumbnail.$.url;
  if (item.enclosure?.url && item.enclosure?.type?.startsWith("image/")) {
    return item.enclosure.url;
  }
  if (item["itunes:image"]?.$?.href) return item["itunes:image"].$.href;
  if (item.content || item.description) {
    const html = item.content || item.description || "";
    const match = html.match(/<img[^>]+src=["']([^"']+)["']/i);
    if (match) return match[1];
  }
  return null;
}

function extractCategories(item) {
  let cats = [];
  if (Array.isArray(item.categories)) cats = item.categories;
  else if (typeof item.categories === "string") cats = [item.categories];
  else if (item.category) cats = Array.isArray(item.category) ? item.category : [item.category];
  return cats
    .map((c) => (typeof c === "object" ? c._ || c["$t"] || "" : c))
    .map((c) => c.toString().trim())
    .filter((c) => c.length > 0);
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Hard timeout ${ms}ms`)), ms)
    ),
  ]);
}


async function processSource(sourceDoc) {
  const source = sourceDoc.data();
  const sourceId = sourceDoc.id;
  const rawFeedUrl = source.stream_link;

  if (!rawFeedUrl) return;

  const feedUrl = encodeRssUrl(rawFeedUrl);

  const t0 = Date.now();
  let feed;
  try {
    feed = await withTimeout(parser.parseURL(feedUrl), 20000);
  } catch (err) {
    if (err.message && (err.message.includes('certificate') || err.message.includes('SSL') || err.message.includes('CERT'))) {
      try {
        const insecureParser = new Parser({
          timeout: 20000,
          headers: { "User-Agent": "RSSAggregator/1.0" },
          requestOptions: { agent: new https.Agent({ rejectUnauthorized: false }) },
          customFields: {
            item: [
              ["media:content", "mediaContent"],
              ["media:thumbnail", "mediaThumbnail"],
              ["enclosure", "enclosure"],
              ["category", "categories"],
            ],
          },
        });
        feed = await withTimeout(insecureParser.parseURL(feedUrl), 20000);
        console.warn(`[${source.name}] SSL bypass used`);
      } catch (retryErr) {
        console.error(`[${source.name}] fetch failed: ${retryErr.message} (${Date.now() - t0}ms)`);
        return;
      }
    } else {
      console.error(`[${source.name}] fetch failed: ${err.message} (${Date.now() - t0}ms)`);
      return;
    }
  }
  const tFetched = Date.now();

  const lastFetched = source.lastFetched?.toDate() || new Date(0);

  const sortedItems = [...feed.items].sort((a, b) => {
    const da = a.pubDate ? new Date(a.pubDate).getTime() : 0;
    const db_ = b.pubDate ? new Date(b.pubDate).getTime() : 0;
    return db_ - da;
  });

  const itemsToProcess = sortedItems
    .filter(item => {
      const pubDate = item.pubDate ? new Date(item.pubDate) : null;
      return !pubDate || pubDate > lastFetched;
    })
    .slice(0, MAX_ARTICLES_PER_SOURCE);

  let batch = db.batch();
  let newCount = 0;
  let skipped = 0;

  for (const item of itemsToProcess) {
    const link = item.link || item.guid;
    if (!link) continue;

    const title = item.title?.trim();
    const imageUrl = extractImageUrl(item);

    if (!title || title.toLowerCase() === "untitled") {
      skipped++;
      continue;
    }
    if (!imageUrl) {
      skipped++;
      continue;
    }

    const categories = extractCategories(item);
    const docId = hashLink(link);
    const slug = slugify(title);
    const docRef = db.collection("rss_articles").doc(docId);

    batch.set(docRef, {
      title,
      slug,
      link,
      description: item.contentSnippet || item.summary || "",
      imageUrl,
      categories,
      sourceGenre: source.genre || "",
      pubDate: toTimestamp(item.pubDate),
      sourceId,
      sourceName: source.name || "",
      sourceLanguage: source.language || "",
      sourceLogo: source.logoUrl || source.logo_url || "",
      fetchedAt: admin.firestore.Timestamp.now(),
    });

    newCount++;
    if (newCount % 499 === 0) {
      await batch.commit();
      batch = db.batch();
    }
  }

  if (newCount % 499 !== 0 && newCount > 0) await batch.commit();

  await sourceDoc.ref.update({
    lastFetched: admin.firestore.Timestamp.now(),
  });

  const tDone = Date.now();
  console.log(
    `[${source.name}] +${newCount} skipped=${skipped} | fetch=${tFetched - t0}ms write=${tDone - tFetched}ms total=${tDone - t0}ms`
  );
}

async function run() {
  const tStart = Date.now();
  console.log("RSS Aggregator started");

  const tQ = Date.now();
  const sourcesSnap = await db
    .collection("rss")
    .where("status", "==", "active")
    .get();
  console.log(`[sources] loaded ${sourcesSnap.size} (${Date.now() - tQ}ms)`);

  if (sourcesSnap.empty) return;

  await Promise.allSettled(sourcesSnap.docs.map(processSource));

  console.log(`[total] run() done in ${Date.now() - tStart}ms`);
}

(async () => {
  try {
    await run();
  } catch (err) {
    console.error("Fatal error:", err);
    process.exitCode = 1;
  } finally {
    const tT = Date.now();
    await admin.app().delete();
    console.log(`[shutdown] firebase closed in ${Date.now() - tT}ms, exiting`);
    process.exit(process.exitCode || 0);
  }
})();
