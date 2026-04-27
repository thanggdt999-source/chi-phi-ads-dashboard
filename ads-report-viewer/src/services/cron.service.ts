import cron from "node-cron";
import { prisma } from "@/lib/prisma";
import { fetchAndSaveReport } from "./report.service";
import { sendTelegramMessage, buildViewerMessage, buildLeaderMessage } from "./telegram.service";

let initialized = false;

export function startCronJobs() {
  if (initialized) return;
  initialized = true;

  // Daily job — 07:00 AM — update YESTERDAY's ads cost + notify
  cron.schedule("0 7 * * *", async () => {
    console.log("[cron] Running daily update for yesterday...");
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);
    await runUpdateAndNotify(yesterday, true);
  });

  // Realtime job — every 10 minutes — update TODAY's ads cost
  cron.schedule("*/10 * * * *", async () => {
    console.log("[cron] Running realtime update for today...");
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    await runUpdateAndNotify(today, false);
  });

  console.log("[cron] Jobs registered: daily 07:00 + every 10 min");
}

async function runUpdateAndNotify(date: Date, notify: boolean) {
  const users = await prisma.user.findMany({
    include: { spreadsheets: { where: { isActive: true }, take: 1 } },
  });

  const leaderSummaries: Record<string, Array<{
    displayName: string;
    telegramId: string;
    totalData: number;
    totalRevenue: number;
    adsPercentage: number;
    costPerResult: number;
  }>> = {};

  for (const user of users) {
    if (user.role !== "viewer") continue;
    const sheet = user.spreadsheets[0];
    if (!sheet) continue;

    try {
      const report = await fetchAndSaveReport(user.id, sheet.id, sheet.url, date);

      if (notify && user.telegramId) {
        const msg = buildViewerMessage(user.displayName, report.date, report, report.products);
        await sendTelegramMessage(user.telegramId, msg);
      }

      if (user.team) {
        if (!leaderSummaries[user.team]) leaderSummaries[user.team] = [];
        leaderSummaries[user.team].push({
          displayName: user.displayName,
          telegramId: user.telegramId,
          totalData: report.totalData,
          totalRevenue: report.totalRevenue,
          adsPercentage: report.adsPercentage,
          costPerResult: report.costPerResult,
        });
      }
    } catch (err) {
      console.error(`[cron] Failed to update report for ${user.username}:`, err);
    }
  }

  if (!notify) return;

  // Notify leaders
  const leaders = await prisma.user.findMany({ where: { role: "leader" } });
  for (const leader of leaders) {
    if (!leader.telegramId || !leader.team) continue;
    const teamData = leaderSummaries[leader.team] ?? [];
    if (teamData.length === 0) continue;

    const msg = buildLeaderMessage(leader.displayName, leader.team, date.toISOString(), teamData);
    await sendTelegramMessage(leader.telegramId, msg);
  }

  // Notify admins — full summary
  const admins = await prisma.user.findMany({ where: { role: "admin" } });
  for (const admin of admins) {
    if (!admin.telegramId) continue;
    const allUsers = Object.values(leaderSummaries).flat();
    if (allUsers.length === 0) continue;
    const msg = buildLeaderMessage(admin.displayName, "All Teams", date.toISOString(), allUsers);
    await sendTelegramMessage(admin.telegramId, msg);
  }
}
