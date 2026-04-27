import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { fetchAndSaveReport } from "@/services/report.service";

/**
 * POST /api/cron/update
 * Called by an external scheduler (e.g., Render cron job, Vercel cron, or node-cron).
 * Protected by a shared secret in the Authorization header.
 */
export async function POST(req: NextRequest) {
  const authHeader = req.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;

  if (!cronSecret || authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  const body = await req.json().catch(() => ({}));
  const mode: "daily" | "realtime" = body.mode === "daily" ? "daily" : "realtime";

  const date = new Date();
  if (mode === "daily") {
    date.setDate(date.getDate() - 1);
  }
  date.setHours(0, 0, 0, 0);

  const users = await prisma.user.findMany({
    where: { role: "viewer" },
    include: { spreadsheets: { where: { isActive: true }, take: 1 } },
  });

  const results: Array<{ username: string; status: string }> = [];

  for (const user of users) {
    const sheet = user.spreadsheets[0];
    if (!sheet) {
      results.push({ username: user.username, status: "skipped (no sheet)" });
      continue;
    }

    try {
      await fetchAndSaveReport(user.id, sheet.id, sheet.url, date);
      results.push({ username: user.username, status: "ok" });
    } catch (err) {
      results.push({
        username: user.username,
        status: `error: ${err instanceof Error ? err.message : String(err)}`,
      });
    }
  }

  return NextResponse.json({ success: true, mode, date: date.toISOString(), results });
}
