"use client";

import { useState, useEffect } from "react";
import { useAuth } from "@/hooks/useAuth";
import { Header } from "@/components/layout/Header";
import { SpreadsheetInput } from "@/components/dashboard/SpreadsheetInput";
import { SummaryCard } from "@/components/dashboard/SummaryCard";
import { ProductTable } from "@/components/dashboard/ProductTable";
import { TelegramConnect } from "@/components/dashboard/TelegramConnect";
import { Card } from "@/components/ui/Card";
import { PageLoader } from "@/components/ui/LoadingSpinner";
import type { ReportSummary } from "@/types";

export default function DashboardPage() {
  const { user, loading: authLoading } = useAuth();

  const [activeUrl, setActiveUrl] = useState("");
  const [urlHistory, setUrlHistory] = useState<string[]>([]);
  const [report, setReport] = useState<ReportSummary | null>(null);
  const [fetchingSheet, setFetchingSheet] = useState(false);
  const [sheetError, setSheetError] = useState("");

  // Telegram connection state (refreshes after connect/disconnect)
  const [tgId, setTgId] = useState<string>("");
  const [tgUsername, setTgUsername] = useState<string>("");

  // Sync tg state from user object
  useEffect(() => {
    if (!user) return;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const u = user as any;
    setTgId(u.telegramId ?? "");
    setTgUsername(u.telegramUsername ?? "");
  }, [user]);

  // Load saved spreadsheet on mount
  useEffect(() => {
    if (!user) return;
    fetch("/api/spreadsheet")
      .then((r) => r.json())
      .then((json) => {
        if (json.success) {
          const { active, history } = json.data;
          if (active?.url) setActiveUrl(active.url);
          if (history?.length) setUrlHistory(history.map((h: { url: string }) => h.url));
        }
      })
      .catch(() => {});
  }, [user]);

  async function handleSheetSubmit(url: string) {
    setSheetError("");
    setFetchingSheet(true);
    try {
      const res = await fetch("/api/spreadsheet", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url }),
      });
      const json = await res.json();
      if (!json.success) {
        setSheetError(json.error ?? "Không thể đọc Sheet");
        return;
      }
      setReport(json.data.report);
      setActiveUrl(url);
      if (!urlHistory.includes(url)) {
        setUrlHistory((prev) => [url, ...prev]);
      }
    } catch {
      setSheetError("Lỗi kết nối khi đọc Sheet");
    } finally {
      setFetchingSheet(false);
    }
  }

  if (authLoading) return <PageLoader />;
  if (!user) return null;

  const fmt = (n: number) => n.toLocaleString("vi-VN");

  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      <Header user={user} />

      <main className="flex-1 max-w-5xl mx-auto w-full px-4 py-8 flex flex-col gap-6">
        {/* Welcome */}
        <div>
          <h2 className="text-xl font-semibold text-gray-900">
            Xin chào, {user.displayName ?? user.username} 👋
          </h2>
          <p className="text-sm text-gray-500 mt-0.5">
            {user.team ? `Team: ${user.team}` : "Báo cáo chi phí Ads của bạn"}
          </p>
        </div>

        {/* Sheet URL Input */}
        <Card>
          <h3 className="text-sm font-semibold text-gray-700 mb-4">📎 Sheet dữ liệu</h3>
          <SpreadsheetInput
            defaultUrl={activeUrl}
            urlHistory={urlHistory}
            onSubmit={handleSheetSubmit}
            loading={fetchingSheet}
          />
          {sheetError && (
            <p className="mt-3 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">{sheetError}</p>
          )}
        </Card>

        {/* Telegram Connect */}
        <Card>
          <h3 className="text-sm font-semibold text-gray-700 mb-4">🔔 Thông báo Telegram</h3>
          <TelegramConnect
            telegramId={tgId}
            telegramUsername={tgUsername}
            onConnected={() => {
              // After user clicks START in Telegram, refresh /api/auth/me to pick up linked status
              setTimeout(async () => {
                const res = await fetch("/api/auth/me");
                const json = await res.json();
                if (json.success) {
                  setTgId(json.data.telegramId ?? "");
                  setTgUsername(json.data.telegramUsername ?? "");
                }
              }, 3000);
            }}
            onDisconnected={() => {
              setTgId("");
              setTgUsername("");
            }}
          />
        </Card>

        {/* Report */}
        {report && (
          <>
            {/* Summary Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <SummaryCard
                label="Tổng Data"
                value={fmt(report.totalData)}
                icon="📦"
                color="blue"
              />
              <SummaryCard
                label="Doanh thu"
                value={`${fmt(report.totalRevenue)} ₫`}
                icon="💰"
                color="green"
              />
              <SummaryCard
                label="% Ads"
                value={`${report.adsPercentage.toFixed(1)}%`}
                icon="📉"
                color={report.adsPercentage > 30 ? "amber" : "green"}
              />
              <SummaryCard
                label="CPA"
                value={`${fmt(report.costPerResult)} ₫`}
                icon="🎯"
                color="purple"
              />
            </div>

            {/* Product Breakdown */}
            <Card padding="none">
              <div className="px-6 pt-5 pb-3 border-b border-gray-50">
                <h3 className="text-sm font-semibold text-gray-700">Chi tiết theo sản phẩm</h3>
              </div>
              <ProductTable products={report.products} />
            </Card>

            <p className="text-xs text-gray-400 text-right">
              Ngày báo cáo: {new Date(report.date).toLocaleDateString("vi-VN")}
            </p>
          </>
        )}

        {!report && !fetchingSheet && (
          <div className="text-center py-16 text-gray-400">
            <p className="text-4xl mb-3">📊</p>
            <p className="text-sm">Dán link Google Sheet để xem báo cáo</p>
          </div>
        )}
      </main>
    </div>
  );
}
