"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import type { UserSession } from "@/types";

interface HeaderProps {
  user: UserSession;
}

export function Header({ user }: HeaderProps) {
  const router = useRouter();
  const [loggingOut, setLoggingOut] = useState(false);

  async function handleLogout() {
    setLoggingOut(true);
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
  }

  const roleLabel = {
    viewer: "Viewer",
    leader: "Leader",
    admin: "Admin",
  }[user.role];

  const roleColor = {
    viewer: "bg-blue-50 text-blue-700",
    leader: "bg-violet-50 text-violet-700",
    admin: "bg-amber-50 text-amber-700",
  }[user.role];

  return (
    <header className="h-14 bg-white border-b border-gray-100 flex items-center px-6 gap-4">
      <Link href="/dashboard" className="font-semibold text-gray-900 text-sm tracking-tight">
        {process.env.NEXT_PUBLIC_APP_NAME ?? "Ads Report Viewer"}
      </Link>

      <nav className="flex items-center gap-1 ml-4">
        <Link
          href="/dashboard"
          className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-50 rounded-md transition-colors"
        >
          Dashboard
        </Link>
        {(user.role === "leader" || user.role === "admin") && (
          <Link
            href="/leader"
            className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-50 rounded-md transition-colors"
          >
            Team
          </Link>
        )}
        {user.role === "admin" && (
          <Link
            href="/admin"
            className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-50 rounded-md transition-colors"
          >
            Admin
          </Link>
        )}
      </nav>

      <div className="ml-auto flex items-center gap-3">
        <span className={`px-2.5 py-1 rounded-full text-xs font-medium ${roleColor}`}>
          {roleLabel}
        </span>
        <span className="text-sm text-gray-700">{user.displayName ?? user.username}</span>
        <button
          onClick={handleLogout}
          disabled={loggingOut}
          className="text-sm text-gray-400 hover:text-gray-700 transition-colors disabled:opacity-50"
        >
          {loggingOut ? "..." : "Logout"}
        </button>
      </div>
    </header>
  );
}
