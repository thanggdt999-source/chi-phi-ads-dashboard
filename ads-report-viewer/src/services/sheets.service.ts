import { google } from "googleapis";
import type { SheetReport, ProductMetric } from "@/types";

function getAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON is not set");

  const credentials = JSON.parse(raw);

  return new google.auth.GoogleAuth({
    credentials,
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets.readonly",
      "https://www.googleapis.com/auth/drive.readonly",
    ],
  });
}

function extractSheetId(url: string): string | null {
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  return match ? match[1] : null;
}

/**
 * Parse a Google Sheet that follows the "Chi phí ADS" format.
 * Reads the first tab named "Chi phí ADS" (or falls back to the first sheet).
 */
export async function parseSheetReport(sheetUrl: string): Promise<SheetReport> {
  const spreadsheetId = extractSheetId(sheetUrl);
  if (!spreadsheetId) throw new Error("Invalid Google Sheet URL");

  const auth = getAuth();
  const sheets = google.sheets({ version: "v4", auth });

  // Get metadata to find the right tab
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const sheetList = meta.data.sheets ?? [];
  const targetSheet =
    sheetList.find((s) => s.properties?.title?.toLowerCase().includes("chi phí ads")) ??
    sheetList[0];

  const sheetName = targetSheet?.properties?.title ?? "Sheet1";
  const spreadsheetTitle = meta.data.properties?.title ?? "";

  // Read all data
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `'${sheetName}'!A1:Z200`,
  });

  const rows: string[][] = (response.data.values ?? []).map((row) =>
    row.map((cell) => String(cell ?? "").trim())
  );

  return parseRows(rows, spreadsheetTitle, sheetName);
}

function parseRows(rows: string[][], spreadsheetTitle: string, _sheetName: string): SheetReport {
  // Infer owner from spreadsheet title (format: "Chi phí ADS - Tên Nhân Viên")
  let owner = spreadsheetTitle;
  if (spreadsheetTitle.includes("-")) {
    const parts = spreadsheetTitle.split("-").map((p) => p.trim());
    owner = parts[parts.length - 1];
  }

  const team = inferTeam(spreadsheetTitle);

  // Find summary row — look for row containing "Tổng" or "Total"
  let totalData = 0;
  let totalRevenue = 0;
  let adsPercentage = 0;
  let costPerResult = 0;

  const products: ProductMetric[] = [];

  for (const row of rows) {
    const label = row[0]?.toLowerCase() ?? "";

    // Summary detection
    if (label.includes("tổng") || label.includes("total")) {
      totalData = parseNumber(row[1]);
      totalRevenue = parseNumber(row[2]);
      adsPercentage = parseNumber(row[3]);
      costPerResult = parseNumber(row[4]);
      continue;
    }

    // Product row: non-empty label, has numeric data in subsequent columns
    const dataVal = parseNumber(row[1]);
    const revenueVal = parseNumber(row[2]);
    if (row[0] && row[0].length > 0 && !label.includes("sản phẩm") && !label.includes("product") && (dataVal > 0 || revenueVal > 0)) {
      products.push({
        productName: row[0],
        data: dataVal,
        revenue: revenueVal,
        adsPercentage: parseNumber(row[3]),
        costPerResult: parseNumber(row[4]),
      });
    }
  }

  return { owner, team, summary: { totalData, totalRevenue, adsPercentage, costPerResult }, products };
}

function inferTeam(text: string): string {
  const lower = text.toLowerCase();
  for (let i = 1; i <= 5; i++) {
    if (lower.includes(`team ${i}`) || lower.includes(`team${i}`)) {
      return `TEAM_${i}`;
    }
  }
  return "";
}

function parseNumber(val: string | undefined): number {
  if (!val) return 0;
  const cleaned = val.replace(/[^0-9.,%-]/g, "").replace(",", ".");
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : num;
}
