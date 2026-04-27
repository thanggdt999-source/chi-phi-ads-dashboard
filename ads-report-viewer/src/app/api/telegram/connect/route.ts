import { NextRequest, NextResponse } from "next/server";
import { verifyToken } from "@/lib/jwt";
import { prisma } from "@/lib/prisma";
import { randomBytes } from "crypto";

const BOT_USERNAME = process.env.TELEGRAM_BOT_USERNAME ?? "";

/**
 * POST /api/telegram/connect
 * Generates a unique connectToken and returns the deep-link URL.
 * The user opens the link in Telegram to start the bot → webhook links their account.
 */
export async function POST(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  if (!BOT_USERNAME) {
    return NextResponse.json(
      { success: false, error: "TELEGRAM_BOT_USERNAME is not configured" },
      { status: 500 }
    );
  }

  // Generate a secure random token (16 hex bytes = 32 chars)
  const connectToken = randomBytes(16).toString("hex");

  await prisma.user.update({
    where: { id: payload.userId },
    data: { connectToken },
  });

  const deepLink = `https://t.me/${BOT_USERNAME}?start=${connectToken}`;

  return NextResponse.json({ success: true, data: { deepLink, connectToken } });
}

/**
 * DELETE /api/telegram/connect
 * Disconnects Telegram from the current user account.
 */
export async function DELETE(req: NextRequest) {
  const token = req.cookies.get("auth_token")?.value;
  const payload = token ? await verifyToken(token) : null;
  if (!payload) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }

  await prisma.user.update({
    where: { id: payload.userId },
    data: { telegramId: "", telegramUsername: "", connectToken: null },
  });

  return NextResponse.json({ success: true });
}
