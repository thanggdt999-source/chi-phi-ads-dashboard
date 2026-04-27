"use client";

import { useState, useEffect } from "react";
import { useAuth } from "@/hooks/useAuth";
import { Header } from "@/components/layout/Header";
import { Card } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";
import { PageLoader } from "@/components/ui/LoadingSpinner";
import type { PublicUser, Role } from "@/types";

const ROLES: Role[] = ["viewer", "leader", "admin"];
const TEAMS = ["", "TEAM_1", "TEAM_2", "TEAM_3", "TEAM_4", "TEAM_5"];

export default function AdminPage() {
  const { user, loading: authLoading } = useAuth();
  const [users, setUsers] = useState<PublicUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState<string | null>(null);
  const [patch, setPatch] = useState<Partial<PublicUser>>({});
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!user) return;
    fetch("/api/admin/users")
      .then((r) => r.json())
      .then((json) => { if (json.success) setUsers(json.data); })
      .finally(() => setLoading(false));
  }, [user]);

  async function saveUser(id: string) {
    setSaving(true);
    try {
      const res = await fetch("/api/admin/users", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id, ...patch }),
      });
      const json = await res.json();
      if (json.success) {
        setUsers((prev) => prev.map((u) => (u.id === id ? { ...u, ...patch } : u)));
        setEditing(null);
        setPatch({});
      }
    } finally {
      setSaving(false);
    }
  }

  if (authLoading || loading) return <PageLoader />;
  if (!user) return null;

  const roleColor: Record<Role, string> = {
    viewer: "bg-blue-50 text-blue-700",
    leader: "bg-violet-50 text-violet-700",
    admin: "bg-amber-50 text-amber-700",
  };

  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      <Header user={user} />
      <main className="flex-1 max-w-5xl mx-auto w-full px-4 py-8 flex flex-col gap-6">

        <div>
          <h2 className="text-xl font-semibold text-gray-900">Quản lý Users</h2>
          <p className="text-sm text-gray-500 mt-0.5">{users.length} tài khoản trong hệ thống</p>
        </div>

        <Card padding="none">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                <th className="text-left px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Username</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Họ tên</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Team</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Role</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Telegram</th>
                <th className="px-6 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {users.map((u) => {
                const isEditing = editing === u.id;
                return (
                  <tr key={u.id} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-3 font-medium text-gray-800">{u.username}</td>
                    <td className="px-4 py-3 text-gray-700">
                      {isEditing ? (
                        <input
                          className="border border-gray-200 rounded px-2 py-1 text-sm w-32"
                          defaultValue={u.displayName}
                          onChange={(e) => setPatch((p) => ({ ...p, displayName: e.target.value }))}
                        />
                      ) : u.displayName}
                    </td>
                    <td className="px-4 py-3">
                      {isEditing ? (
                        <select
                          className="border border-gray-200 rounded px-2 py-1 text-sm"
                          defaultValue={u.team}
                          onChange={(e) => setPatch((p) => ({ ...p, team: e.target.value }))}
                        >
                          {TEAMS.map((t) => <option key={t} value={t}>{t || "—"}</option>)}
                        </select>
                      ) : (u.team || "—")}
                    </td>
                    <td className="px-4 py-3">
                      {isEditing ? (
                        <select
                          className="border border-gray-200 rounded px-2 py-1 text-sm"
                          defaultValue={u.role}
                          onChange={(e) => setPatch((p) => ({ ...p, role: e.target.value as Role }))}
                        >
                          {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                        </select>
                      ) : (
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${roleColor[u.role]}`}>
                          {u.role}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">
                      {u.telegramUsername
                        ? <span className="text-emerald-600 font-medium">@{u.telegramUsername}</span>
                        : u.telegramId
                          ? <span className="text-emerald-600 font-medium">ID {u.telegramId}</span>
                          : <span className="text-gray-300">—</span>
                      }
                    </td>
                    <td className="px-6 py-3">
                      {isEditing ? (
                        <div className="flex gap-2">
                          <Button size="sm" loading={saving} onClick={() => saveUser(u.id)}>Lưu</Button>
                          <Button size="sm" variant="ghost" onClick={() => { setEditing(null); setPatch({}); }}>Hủy</Button>
                        </div>
                      ) : (
                        <Button size="sm" variant="secondary" onClick={() => { setEditing(u.id); setPatch({}); }}>
                          Sửa
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Card>

      </main>
    </div>
  );
}
