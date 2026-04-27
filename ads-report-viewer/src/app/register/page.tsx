"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";
import { Card } from "@/components/ui/Card";

export default function RegisterPage() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [fields, setFields] = useState({
    username: "",
    password: "",
    confirmPassword: "",
    displayName: "",
    team: "",
  });

  function set(key: keyof typeof fields) {
    return (e: React.ChangeEvent<HTMLInputElement>) =>
      setFields((prev) => ({ ...prev, [key]: e.target.value }));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");

    if (fields.password !== fields.confirmPassword) {
      setError("Mật khẩu xác nhận không khớp");
      return;
    }

    setLoading(true);
    try {
      const res = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(fields),
      });

      const data = await res.json();
      if (!data.success) {
        setError(data.error ?? "Đăng ký thất bại");
        return;
      }

      router.push("/login");
    } catch {
      setError("Không thể kết nối đến server");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-900 via-blue-950 to-slate-900 px-4 py-10">
      <div className="w-full max-w-sm">
        <div className="mb-8 text-center">
          <h1 className="text-2xl font-semibold text-white">Tạo tài khoản</h1>
          <p className="text-sm text-slate-400 mt-1">Điền đầy đủ thông tin bên dưới</p>
        </div>

        <Card>
          <form onSubmit={handleSubmit} className="flex flex-col gap-4">
            <Input label="Tên đăng nhập" type="text" value={fields.username} onChange={set("username")} required autoFocus />
            <Input label="Họ tên hiển thị" type="text" value={fields.displayName} onChange={set("displayName")} placeholder="Tên hiển thị (tùy chọn)" />
            <Input label="Mật khẩu" type="password" value={fields.password} onChange={set("password")} required autoComplete="new-password" />
            <Input label="Xác nhận mật khẩu" type="password" value={fields.confirmPassword} onChange={set("confirmPassword")} required autoComplete="new-password" />
            <Input label="Team" type="text" value={fields.team} onChange={set("team")} placeholder="VD: TEAM_1 (tùy chọn)" />

            {error && (
              <div className="rounded-lg bg-red-50 border border-red-200 px-3.5 py-2.5 text-sm text-red-700">
                {error}
              </div>
            )}

            <Button type="submit" loading={loading} size="lg" className="mt-1 w-full">
              Đăng ký
            </Button>
          </form>
        </Card>

        <p className="mt-5 text-center text-sm text-slate-400">
          Đã có tài khoản?{" "}
          <Link href="/login" className="text-blue-400 hover:text-blue-300 font-medium">
            Đăng nhập
          </Link>
        </p>
      </div>
    </div>
  );
}
