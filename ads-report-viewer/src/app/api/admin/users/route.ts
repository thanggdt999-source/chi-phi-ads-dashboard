import { NextRequest, NextResponse } from "next/server";
import { verifyToken } from "@/lib/jwt";
import { prisma } from "@/lib/prisma";

// GET /api/admin/users — admin only
export async function GET(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload || payload.role !== "admin") {
    return NextResponse.json({ success: false, error: "Forbidden" }, { status: 403 });
  }

  const users = await prisma.user.findMany({
    orderBy: { createdAt: "desc" },
    select: {
      id: true,
      username: true,
      displayName: true,
      team: true,
      role: true,
      telegramId: true,
      createdAt: true,
    },
  });

  return NextResponse.json({ success: true, data: users });
}

// PATCH /api/admin/users — update user role/team (admin only)
export async function PATCH(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload || payload.role !== "admin") {
    return NextResponse.json({ success: false, error: "Forbidden" }, { status: 403 });
  }

  const body = await req.json();
  const { id, role, team, telegramId, displayName } = body;

  if (!id) {
    return NextResponse.json({ success: false, error: "User ID required" }, { status: 400 });
  }

  const updated = await prisma.user.update({
    where: { id: String(id) },
    data: {
      ...(role ? { role } : {}),
      ...(team !== undefined ? { team } : {}),
      ...(telegramId !== undefined ? { telegramId } : {}),
      ...(displayName ? { displayName } : {}),
    },
    select: { id: true, username: true, role: true, team: true },
  });

  return NextResponse.json({ success: true, data: updated });
}
