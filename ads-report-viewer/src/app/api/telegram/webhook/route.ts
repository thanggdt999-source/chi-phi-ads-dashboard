import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { sendTelegramMessage } from "@/services/telegram.service";

/**
 * POST /api/telegram/webhook
 * Telegram sends updates here (set via setWebhook).
 *
 * Expected env:
 *   TELEGRAM_BOT_TOKEN      — bot token
 *   TELEGRAM_WEBHOOK_SECRET — optional secret for X-Telegram-Bot-Api-Secret-Token header
 */
export async function POST(req: NextRequest) {
  // Optional: verify Telegram webhook secret
  const webhookSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
  if (webhookSecret) {
    const headerSecret = req.headers.get("x-telegram-bot-api-secret-token");
    if (headerSecret !== webhookSecret) {
      return NextResponse.json({ ok: false }, { status: 403 });
    }
  }

  let body: TelegramUpdate;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ ok: true }); // silently ignore malformed
  }

  const message = body?.message;
  if (!message || !message.text) {
    return NextResponse.json({ ok: true }); // not a text message, ignore
  }

  const tgUserId = String(message.from?.id ?? "");
  const tgUsername = message.from?.username ?? "";
  const text = message.text.trim();

  // Handle /start <connectToken>
  if (text.startsWith("/start ")) {
    const connectToken = text.slice("/start ".length).trim();

    if (!connectToken) {
      await sendTelegramMessage(tgUserId, "❌ Link kết nối không hợp lệ. Vui lòng thử lại từ ứng dụng.");
      return NextResponse.json({ ok: true });
    }

    const user = await prisma.user.findUnique({ where: { connectToken } });

    if (!user) {
      await sendTelegramMessage(
        tgUserId,
        "❌ Link kết nối đã hết hạn hoặc không tồn tại. Hãy tạo link mới từ ứng dụng."
      );
      return NextResponse.json({ ok: true });
    }

    // Link Telegram account
    await prisma.user.update({
      where: { id: user.id },
      data: {
        telegramId: tgUserId,
        telegramUsername: tgUsername,
        connectToken: null, // consume the token
      },
    });

    const displayName = user.displayName || user.username;
    const tgDisplay = tgUsername ? `@${tgUsername}` : `ID ${tgUserId}`;

    await sendTelegramMessage(
      tgUserId,
      `✅ Kết nối thành công!\n\nTài khoản <b>${displayName}</b> đã được liên kết với Telegram ${tgDisplay}.\n\nBạn sẽ nhận thông báo báo cáo chi phí Ads tại đây.`
    );

    return NextResponse.json({ ok: true });
  }

  // Handle /start without token (direct bot open)
  if (text === "/start") {
    await sendTelegramMessage(
      tgUserId,
      "👋 Xin chào! Để kết nối tài khoản, hãy mở ứng dụng và nhấn <b>Connect Telegram</b>."
    );
    return NextResponse.json({ ok: true });
  }

  return NextResponse.json({ ok: true });
}

// ── Types ──────────────────────────────────────────────────

interface TelegramUser {
  id: number;
  username?: string;
  first_name?: string;
}

interface TelegramMessage {
  message_id: number;
  from?: TelegramUser;
  text?: string;
}

interface TelegramUpdate {
  update_id: number;
  message?: TelegramMessage;
}
