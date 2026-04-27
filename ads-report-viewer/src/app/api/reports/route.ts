import { NextRequest, NextResponse } from "next/server";
import { verifyToken } from "@/lib/jwt";
import { getReportsByUser, getReportsByTeam, getAllReports } from "@/services/report.service";

export async function GET(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(req.url);
  const from = searchParams.get("from") ? new Date(searchParams.get("from")!) : undefined;
  const to = searchParams.get("to") ? new Date(searchParams.get("to")!) : undefined;
  const targetUserId = searchParams.get("userId");
  const targetTeam = searchParams.get("team");

  const role = payload.role;

  // Viewer — only own data
  if (role === "viewer") {
    const reports = await getReportsByUser(payload.userId, from, to);
    return NextResponse.json({ success: true, data: reports });
  }

  // Leader — can query by team (own team only)
  if (role === "leader") {
    if (targetTeam && targetTeam !== payload.team) {
      return NextResponse.json({ success: false, error: "Access denied" }, { status: 403 });
    }
    const reports = targetTeam
      ? await getReportsByTeam(targetTeam)
      : await getReportsByTeam(payload.team);
    return NextResponse.json({ success: true, data: reports });
  }

  // Admin — full access
  if (targetUserId) {
    const reports = await getReportsByUser(targetUserId, from, to);
    return NextResponse.json({ success: true, data: reports });
  }
  if (targetTeam) {
    const reports = await getReportsByTeam(targetTeam);
    return NextResponse.json({ success: true, data: reports });
  }

  const reports = await getAllReports();
  return NextResponse.json({ success: true, data: reports });
}
