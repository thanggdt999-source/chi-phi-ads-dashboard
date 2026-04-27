import { NextRequest, NextResponse } from "next/server";
import { registerUser } from "@/services/auth.service";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { username, password, confirmPassword, team, telegramId, displayName } = body;

    if (!username || !password || !confirmPassword) {
      return NextResponse.json(
        { success: false, error: "Username, password and confirmPassword are required" },
        { status: 400 }
      );
    }

    await registerUser({
      username: String(username).trim(),
      password: String(password),
      confirmPassword: String(confirmPassword),
      team: String(team ?? "").trim(),
      telegramId: String(telegramId ?? "").trim(),
      displayName: String(displayName ?? "").trim(),
    });

    return NextResponse.json({ success: true });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Registration failed";
    return NextResponse.json({ success: false, error: message }, { status: 400 });
  }
}
