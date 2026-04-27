import { NextRequest, NextResponse } from "next/server";
import { verifyToken } from "@/lib/jwt";
import {
  saveOrUpdateSpreadsheet,
  getActiveSpreadsheet,
  getUserSpreadsheets,
} from "@/services/spreadsheet.service";
import { fetchAndSaveReport } from "@/services/report.service";

// GET — return active spreadsheet + URL history for current user
export async function GET(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  const [active, history] = await Promise.all([
    getActiveSpreadsheet(payload.userId),
    getUserSpreadsheets(payload.userId),
  ]);

  return NextResponse.json({ success: true, data: { active, history } });
}

// POST — submit a sheet URL, parse and fetch report for today
export async function POST(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  const { url } = await req.json();
  if (!url || typeof url !== "string") {
    return NextResponse.json({ success: false, error: "Sheet URL is required" }, { status: 400 });
  }

  // Basic URL validation
  if (!url.includes("docs.google.com/spreadsheets")) {
    return NextResponse.json({ success: false, error: "Must be a valid Google Sheets URL" }, { status: 400 });
  }

  const sheet = await saveOrUpdateSpreadsheet(payload.userId, url.trim());

  // Fetch today's report immediately
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const report = await fetchAndSaveReport(payload.userId, sheet.id, url.trim(), today);

  return NextResponse.json({ success: true, data: { sheet, report } });
}
