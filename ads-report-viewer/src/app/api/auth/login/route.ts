import { NextRequest, NextResponse } from "next/server";
import { loginUser } from "@/services/auth.service";

export async function POST(req: NextRequest) {
  try {
    const { username, password } = await req.json();

    if (!username || !password) {
      return NextResponse.json({ success: false, error: "Username and password required" }, { status: 400 });
    }

    const token = await loginUser({ username: String(username).trim(), password: String(password) });

    const res = NextResponse.json({ success: true });
    res.cookies.set("auth_token", token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: 60 * 10, // 10 minutes
      path: "/",
    });

    return res;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Login failed";
    return NextResponse.json({ success: false, error: message }, { status: 401 });
  }
}
