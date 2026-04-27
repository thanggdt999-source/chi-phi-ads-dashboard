export async function sendTelegramMessage(chatId: string, text: string): Promise<void> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token || !chatId) return;

  const url = `https://api.telegram.org/bot${token}/sendMessage`;

  await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "HTML",
    }),
  });
}

export function buildViewerMessage(
  displayName: string,
  date: string,
  summary: {
    totalData: number;
    totalRevenue: number;
    adsPercentage: number;
    costPerResult: number;
  },
  products: Array<{
    productName: string;
    data: number;
    revenue: number;
    adsPercentage: number;
    costPerResult: number;
  }>
): string {
  const d = new Date(date).toLocaleDateString("vi-VN");
  let msg = `<b>📊 Báo cáo chi phí Ads</b>\n`;
  msg += `👤 ${displayName} — <i>${d}</i>\n\n`;
  msg += `📦 Tổng data: <b>${summary.totalData.toLocaleString()}</b>\n`;
  msg += `💰 Doanh thu: <b>${summary.totalRevenue.toLocaleString()} ₫</b>\n`;
  msg += `📉 % Ads: <b>${summary.adsPercentage.toFixed(1)}%</b>\n`;
  msg += `🎯 CPA: <b>${summary.costPerResult.toLocaleString()} ₫</b>\n`;

  if (products.length > 0) {
    msg += `\n<b>Chi tiết sản phẩm:</b>\n`;
    for (const p of products) {
      msg += `• ${p.productName}: ${p.data.toLocaleString()} data | ${p.revenue.toLocaleString()} ₫ | ${p.adsPercentage.toFixed(1)}% | CPA ${p.costPerResult.toLocaleString()}\n`;
    }
  }

  return msg;
}

export function buildLeaderMessage(
  leaderName: string,
  team: string,
  date: string,
  userReports: Array<{
    displayName: string;
    totalData: number;
    totalRevenue: number;
    adsPercentage: number;
    costPerResult: number;
  }>
): string {
  const d = new Date(date).toLocaleDateString("vi-VN");
  let msg = `<b>📋 Báo cáo Team ${team}</b>\n`;
  msg += `👑 ${leaderName} — <i>${d}</i>\n\n`;

  const totals = userReports.reduce(
    (acc, r) => ({
      totalData: acc.totalData + r.totalData,
      totalRevenue: acc.totalRevenue + r.totalRevenue,
    }),
    { totalData: 0, totalRevenue: 0 }
  );

  msg += `📦 Tổng data team: <b>${totals.totalData.toLocaleString()}</b>\n`;
  msg += `💰 Tổng doanh thu: <b>${totals.totalRevenue.toLocaleString()} ₫</b>\n\n`;

  msg += `<b>Theo nhân viên:</b>\n`;
  for (const r of userReports) {
    msg += `• ${r.displayName}: ${r.totalData.toLocaleString()} | ${r.totalRevenue.toLocaleString()} ₫ | ${r.adsPercentage.toFixed(1)}% ads\n`;
  }

  return msg;
}
