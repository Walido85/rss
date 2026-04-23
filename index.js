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
});

function hashLink(link) {
  return crypto.createHash("md5").update(link).digest("hex");
}

function toTimestamp(dateStr) {
  if (!dateStr) return admin.firestore.Timestamp.now();
  const d = new Date(dateStr);
  return isNaN(d) ? admin.firestore.Timestamp.now() : admin.firestore.Timestamp.fromDate(d);
}

async function run() {
  console.log("🚀 RSS Aggregator started");

  // Read from your existing "rss" collection
  const sourcesSnap = await db
    .collection("rss")
    .where("status", "==", "active")
    .get();

  if (sourcesSnap.empty) {
    console.log("No active sources found.");
    return;
  }

  console.log(`📡 Found ${sourcesSnap.size} active source(s)`);

  for (const sourceDoc of sourcesSnap.docs) {
    const source = sourceDoc.data();
    const sourceId = sourceDoc.id;

    // Use stream_link as the RSS feed URL
    const feedUrl = source.stream_link;
    if (!feedUrl) {
      console.log(`  ⚠️ Skipping ${source.name} — no stream_link`);
      continue;
    }

    console.log(`\n🔗 Fetching: ${source.name} — ${feedUrl}`);

    let feed;
    try {
      feed = await parser.parseURL(feedUrl);
    } catch (err) {
      console.error(`  ❌ Failed to fetch ${feedUrl}:`, err.message);
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

      const docId = hashLink(link);
      const docRef = db.collection("rss_articles").doc(docId);

      batch.set(docRef, {
        title: item.title || "Untitled",
        link,
        description: item.contentSnippet || item.summary || "",
        pubDate: toTimestamp(item.pubDate),
        sourceId,
        sourceName: source.name || "",
        sourceGenre: source.genre || "",
        sourceLanguage: source.language || "",
        sourceLogo: source.logoUrl || source.logo_url || "",
        fetchedAt: admin.firestore.Timestamp.now(),
      });

      newCount++;

      if (newCount % 499 === 0) {
        await batch.commit();
        console.log(`  ✅ Committed 499 articles`);
      }
    }

    if (newCount > 0) {
      await batch.commit();
    }

    // Save lastFetched to avoid re-fetching same articles
    await sourceDoc.ref.update({
      lastFetched: admin.firestore.Timestamp.now(),
    });

    console.log(`  ✅ Added ${newCount} new article(s) from ${source.name}`);
  }

  console.log("\n✅ Done.");
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
