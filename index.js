import admin from "firebase-admin";
import Parser from "rss-parser";
import crypto from "crypto";

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

async function cleanOldArticles(monthsOld = 3) {
  const t0 = Date.now();
  console.log(`[clean] starting...`);
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - monthsOld);
  const cutoffTimestamp = admin.firestore.Timestamp.fromDate(cutoff);

  const oldSnap = await db
    .collection("rss_articles")
    .where("fetchedAt", "<", cutoffTimestamp)
    .get();

  if (oldSnap.empty) {
    console.log(`[clean] no old articles (${Date.now() - t0}ms)`);
    return;
  }

  let batch = db.batch();
  let count = 0;
  for (const doc of oldSnap.docs) {
    batch.delete(doc.ref);
    count++;
    if (count % 499 === 0) {
      await batch.commit();
      batch = db.batch();
    }
  }
  if (count % 499 !== 0) await batch.commit();
  console.log(`[clean] deleted ${count} (${Date.now() - t0}ms)`);
}

async function processSource(sourceDoc) {
  const source = sourceDoc.data();
  const sourceId = sourceDoc.id;
  const feedUrl = source.stream_link;

  if (!feedUrl) return;

  const t0 = Date.now();
  let feed;
  try {
    feed = await withTimeout(parser.parseURL(feedUrl), 20000);
  } catch (err) {
    console.error(`[${source.name}] fetch failed: ${err.message} (${Date.now() - t0}ms)`);
    return;
  }
  const tFetched = Date.now();

  const lastFetched = source.lastFetched?.toDate() || new Date(0);
  let batch = db.batch();
  let newCount = 0;

  for (const item of feed.items) {
    const link = item.link || item.guid;
    if (!link) continue;

    const pubDate = item.pubDate ? new Date(item.pubDate) : null;
    if (pubDate && pubDate <= lastFetched) continue;

    const imageUrl = extractImageUrl(item);
    const categories = extractCategories(item);
    const docId = hashLink(link);
    const slug = slugify(item.title || docId);
    const docRef = db.collection("rss_articles").doc(docId);

    batch.set(docRef, {
      title: item.title || "Untitled",
      slug,
      link,
      description: item.contentSnippet || item.summary || "",
      imageUrl: imageUrl || null,
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
    `[${source.name}] +${newCount} | fetch=${tFetched - t0}ms write=${tDone - tFetched}ms total=${tDone - t0}ms`
  );
}

async function run() {
  const tStart = Date.now();
  console.log("RSS Aggregator started");

  await cleanOldArticles(3);

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
