import { prisma } from "@/lib/prisma";
import { parseSheetReport } from "./sheets.service";

export async function saveOrUpdateSpreadsheet(userId: string, url: string) {
  // Deactivate previous active spreadsheets for this user
  await prisma.spreadsheet.updateMany({
    where: { userId, isActive: true },
    data: { isActive: false },
  });

  // Parse the sheet to get owner/team metadata
  let parsedOwner = "";
  let team = "";
  try {
    const parsed = await parseSheetReport(url);
    parsedOwner = parsed.owner;
    team = parsed.team;
  } catch {
    // Non-blocking — still save the URL even if parsing fails
  }

  const sheet = await prisma.spreadsheet.upsert({
    where: { id: `${userId}:${url}`.slice(0, 25) }, // fallback key
    update: { isActive: true, parsedOwner, team },
    create: { userId, url, parsedOwner, team, isActive: true },
  });

  return sheet;
}

export async function getActiveSpreadsheet(userId: string) {
  return prisma.spreadsheet.findFirst({
    where: { userId, isActive: true },
    orderBy: { createdAt: "desc" },
  });
}

export async function getUserSpreadsheets(userId: string) {
  return prisma.spreadsheet.findMany({
    where: { userId },
    orderBy: { createdAt: "desc" },
    select: { id: true, url: true, parsedOwner: true, team: true, isActive: true, createdAt: true },
  });
}
