import admin from "firebase-admin";
import Parser from "rss-parser";
import crypto from "crypto";

// ─── Firebase Init ───────────────────────────────────────────────
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const parser = new Parser({
  timeout: 10000,
  headers: { "User-Agent": "RSSAggregator/1.0" },
});

// ─── Helpers ─────────────────────────────────────────────────────
function hashLink(link) {
  return crypto.createHash("md5").update(link).digest("hex");
}

function toTimestamp(dateStr) {
  if (!dateStr) return admin.firestore.Timestamp.now();
  const d = new Date(dateStr);
  return isNaN(d) ? admin.firestore.Timestamp.now() : admin.firestore.Timestamp.fromDate(d);
}

// ─── Main ─────────────────────────────────────────────────────────
async function run() {
  console.log("🚀 RSS Aggregator started");

  // 1. Fetch active sources (1 read per source document)
  const sourcesSnap = await db
    .collection("rss_sources")
    .where("active", "==", true)
    .get();

  if (sourcesSnap.empty) {
    console.log("No active sources found.");
    return;
  }

  console.log(`📡 Found ${sourcesSnap.size} active source(s)`);

  for (const sourceDoc of sourcesSnap.docs) {
    const source = sourceDoc.data();
    const sourceId = sourceDoc.id;

    console.log(`\n🔗 Fetching: ${source.name} — ${source.url}`);

    let feed;
    try {
      feed = await parser.parseURL(source.url);
    } catch (err) {
      console.error(`  ❌ Failed to fetch ${source.url}:`, err.message);
      continue;
    }

    const lastFetched = source.lastFetched?.toDate() || new Date(0);
    const batch = db.batch();
    let newCount = 0;

    for (const item of feed.items) {
      const link = item.link || item.guid;
      if (!link) continue;

      // Only process articles newer than lastFetched (quota saver)
      const pubDate = item.pubDate ? new Date(item.pubDate) : null;
      if (pubDate && pubDate <= lastFetched) continue;

      const docId = hashLink(link);
      const docRef = db.collection("rss_articles").doc(docId);

      batch.set(
        docRef,
        {
          title: item.title || "Untitled",
          link,
          description: item.contentSnippet || item.summary || "",
          pubDate: toTimestamp(item.pubDate),
          sourceId,
          sourceName: source.name || "",
          fetchedAt: admin.firestore.Timestamp.now(),
        },
        { merge: false } // don't overwrite existing = saves writes
      );

      newCount++;

      // Firestore batch limit = 500
      if (newCount % 499 === 0) {
        await batch.commit();
        console.log(`  ✅ Committed 499 articles`);
      }
    }

    if (newCount > 0) {
      await batch.commit();
    }

    // Update lastFetched on source (1 write per source)
    await sourceDoc.ref.update({
      lastFetched: admin.firestore.Timestamp.now(),
    });

    console.log(`  ✅ Added ${newCount} new article(s)`);
  }

  console.log("\n✅ Done.");
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
