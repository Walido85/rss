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

async function cleanOldArticles(monthsOld = 3) {
  console.log(`Cleaning articles older than ${monthsOld} months...`);
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - monthsOld);
  const cutoffTimestamp = admin.firestore.Timestamp.fromDate(cutoff);

  const oldSnap = await db
    .collection("rss_articles")
    .where("pubDate", "<", cutoffTimestamp)
    .get();

  if (oldSnap.empty) {
    console.log("No old articles to delete.");
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
  console.log(`Deleted ${count} old article(s).`);
}

async function run() {
  console.log("RSS Aggregator started");

  await cleanOldArticles(3);

  const sourcesSnap = await db
    .collection("rss")
    .where("status", "==", "active")
    .get();

  if (sourcesSnap.empty) {
    console.log("No active sources found.");
    return;
  }

  console.log(`Found ${sourcesSnap.size} active source(s)`);

  for (const sourceDoc of sourcesSnap.docs) {
    const source = sourceDoc.data();
    const sourceId = sourceDoc.id;

    const feedUrl = source.stream_link;
    if (!feedUrl) {
      console.log(`Skipping ${source.name} — no stream_link`);
      continue;
    }

    console.log(`Fetching: ${source.name} — ${feedUrl}`);

    let feed;
    try {
      feed = await parser.parseURL(feedUrl);
    } catch (err) {
      console.error(`Failed to fetch ${feedUrl}:`, err.message);
      continue;
    }

    const lastFetched = source.lastFetched?.toDate() || new Date(0);
    const batch = db.batch();
    let newCount = 0;

    for (const item of feed.items) {
      const link = item.link || item.guid;
      if (!link) continue;

      const pubDate = item.pubDate ? new Date(item.pubDate) : null;
      if (pubDate && pubDate <= lastFetched) continue;

      const imageUrl = extractImageUrl(item);
      const docId = hashLink(link);
      const slug = slugify(item.title || docId);
      const docRef = db.collection("rss_articles").doc(docId);

      batch.set(docRef, {
        title: item.title || "Untitled",
        slug,
        link,
        description: item.contentSnippet || item.summary || "",
        imageUrl: imageUrl || null,
        pubDate: toTimestamp(item.pubDate),
        sourceId,
        sourceName: source.name || "",
        sourceGenre: source.genre || "",
        sourceLanguage: source.language || "",
        sourceLogo: source.logoUrl || source.logo_url || "",
        fetchedAt: admin.firestore.Timestamp.now(),
      });

      newCount++;
      if (newCount % 499 === 0) await batch.commit();
    }

    if (newCount > 0) await batch.commit();

    await sourceDoc.ref.update({
      lastFetched: admin.firestore.Timestamp.now(),
    });

    console.log(`Added ${newCount} new article(s) from ${source.name}`);
  }

  console.log("Done.");
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
