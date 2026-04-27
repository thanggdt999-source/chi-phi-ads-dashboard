import { prisma } from "@/lib/prisma";
import { parseSheetReport } from "./sheets.service";
import type { ReportSummary } from "@/types";

/**
 * Parse a spreadsheet and upsert the report for a given date.
 */
export async function fetchAndSaveReport(
  userId: string,
  spreadsheetId: string,
  sheetUrl: string,
  date: Date
): Promise<ReportSummary> {
  const parsed = await parseSheetReport(sheetUrl);

  const report = await prisma.report.upsert({
    where: { userId_date: { userId, date } },
    update: {
      totalData: parsed.summary.totalData,
      totalRevenue: parsed.summary.totalRevenue,
      adsPercentage: parsed.summary.adsPercentage,
      costPerResult: parsed.summary.costPerResult,
      rawJson: parsed as object,
      spreadsheetId,
    },
    create: {
      userId,
      spreadsheetId,
      date,
      totalData: parsed.summary.totalData,
      totalRevenue: parsed.summary.totalRevenue,
      adsPercentage: parsed.summary.adsPercentage,
      costPerResult: parsed.summary.costPerResult,
      rawJson: parsed as object,
    },
  });

  // Replace product reports
  await prisma.productReport.deleteMany({ where: { reportId: report.id } });
  if (parsed.products.length > 0) {
    await prisma.productReport.createMany({
      data: parsed.products.map((p) => ({
        reportId: report.id,
        productName: p.productName,
        data: p.data,
        revenue: p.revenue,
        adsPercentage: p.adsPercentage,
        costPerResult: p.costPerResult,
      })),
    });
  }

  return {
    date: date.toISOString(),
    ...parsed.summary,
    products: parsed.products,
  };
}

export async function getReportsByUser(
  userId: string,
  from?: Date,
  to?: Date
): Promise<ReportSummary[]> {
  const where: Record<string, unknown> = { userId };
  if (from || to) {
    where.date = {
      ...(from ? { gte: from } : {}),
      ...(to ? { lte: to } : {}),
    };
  }

  const reports = await prisma.report.findMany({
    where,
    orderBy: { date: "desc" },
    include: { productReports: true },
  });

  return reports.map((r) => ({
    date: r.date.toISOString(),
    totalData: r.totalData,
    totalRevenue: r.totalRevenue,
    adsPercentage: r.adsPercentage,
    costPerResult: r.costPerResult,
    products: r.productReports.map((p) => ({
      productName: p.productName,
      data: p.data,
      revenue: p.revenue,
      adsPercentage: p.adsPercentage,
      costPerResult: p.costPerResult,
    })),
  }));
}

export async function getReportsByTeam(team: string) {
  const users = await prisma.user.findMany({ where: { team }, select: { id: true } });
  const userIds = users.map((u) => u.id);

  return prisma.report.findMany({
    where: { userId: { in: userIds } },
    orderBy: { date: "desc" },
    include: { productReports: true, user: { select: { displayName: true, username: true } } },
  });
}

export async function getAllReports() {
  return prisma.report.findMany({
    orderBy: { date: "desc" },
    include: {
      productReports: true,
      user: { select: { displayName: true, username: true, team: true } },
    },
  });
}
