"use client";

import { useState, useEffect } from "react";
import { useAuth } from "@/hooks/useAuth";
import { Header } from "@/components/layout/Header";
import { SummaryCard } from "@/components/dashboard/SummaryCard";
import { ProductTable } from "@/components/dashboard/ProductTable";
import { Card } from "@/components/ui/Card";
import { PageLoader } from "@/components/ui/LoadingSpinner";

interface UserReport {
  userId: string;
  user: { displayName: string; username: string };
  date: string;
  totalData: number;
  totalRevenue: number;
  adsPercentage: number;
  costPerResult: number;
  productReports: Array<{
    productName: string;
    data: number;
    revenue: number;
    adsPercentage: number;
    costPerResult: number;
  }>;
}

export default function LeaderPage() {
  const { user, loading: authLoading } = useAuth();
  const [reports, setReports] = useState<UserReport[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<UserReport | null>(null);

  useEffect(() => {
    if (!user) return;
    setLoading(true);
    fetch("/api/reports")
      .then((r) => r.json())
      .then((json) => {
        if (json.success) setReports(json.data);
      })
      .finally(() => setLoading(false));
  }, [user]);

  if (authLoading || loading) return <PageLoader />;
  if (!user) return null;

  const fmt = (n: number) => n.toLocaleString("vi-VN");

  const teamTotal = {
    totalData: reports.reduce((s, r) => s + r.totalData, 0),
    totalRevenue: reports.reduce((s, r) => s + r.totalRevenue, 0),
    avgAds: reports.length ? reports.reduce((s, r) => s + r.adsPercentage, 0) / reports.length : 0,
  };

  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      <Header user={user} />
      <main className="flex-1 max-w-5xl mx-auto w-full px-4 py-8 flex flex-col gap-6">

        <div>
          <h2 className="text-xl font-semibold text-gray-900">Dashboard Team</h2>
          <p className="text-sm text-gray-500 mt-0.5">{user.team}</p>
        </div>

        {/* Team Summary */}
        <div className="grid grid-cols-3 gap-4">
          <SummaryCard label="Tổng Data Team" value={fmt(teamTotal.totalData)} icon="📦" color="blue" />
          <SummaryCard label="Tổng Doanh thu" value={`${fmt(teamTotal.totalRevenue)} ₫`} icon="💰" color="green" />
          <SummaryCard label="Avg % Ads" value={`${teamTotal.avgAds.toFixed(1)}%`} icon="📉" color="amber" />
        </div>

        {/* User list */}
        <Card padding="none">
          <div className="px-6 py-4 border-b border-gray-50">
            <h3 className="text-sm font-semibold text-gray-700">Báo cáo theo nhân viên</h3>
          </div>
          {reports.length === 0 ? (
            <p className="text-center py-12 text-gray-400 text-sm">Chưa có dữ liệu</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Nhân viên</th>
                  <th className="text-right px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Data</th>
                  <th className="text-right px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Doanh thu</th>
                  <th className="text-right px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">% Ads</th>
                  <th className="text-right px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">CPA</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {reports.map((r, i) => (
                  <tr
                    key={i}
                    className="hover:bg-gray-50 cursor-pointer transition-colors"
                    onClick={() => setSelected(selected?.userId === r.userId ? null : r)}
                  >
                    <td className="px-6 py-3 font-medium text-gray-800">{r.user?.displayName ?? r.userId}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{fmt(r.totalData)}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{fmt(r.totalRevenue)} ₫</td>
                    <td className="px-4 py-3 text-right">
                      <span className={`font-medium ${r.adsPercentage > 30 ? "text-red-500" : "text-emerald-600"}`}>
                        {r.adsPercentage.toFixed(1)}%
                      </span>
                    </td>
                    <td className="px-6 py-3 text-right text-gray-700">{fmt(r.costPerResult)} ₫</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>

        {/* Expanded product detail */}
        {selected && (
          <Card>
            <h3 className="text-sm font-semibold text-gray-700 mb-4">
              Chi tiết sản phẩm — {selected.user?.displayName}
            </h3>
            <ProductTable
              products={selected.productReports.map((p) => ({
                productName: p.productName,
                data: p.data,
                revenue: p.revenue,
                adsPercentage: p.adsPercentage,
                costPerResult: p.costPerResult,
              }))}
            />
          </Card>
        )}
      </main>
    </div>
  );
}
