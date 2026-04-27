import type { ProductMetric } from "@/types";

interface ProductTableProps {
  products: ProductMetric[];
}

export function ProductTable({ products }: ProductTableProps) {
  if (products.length === 0) {
    return (
      <div className="text-center py-10 text-gray-400 text-sm">
        Chưa có dữ liệu sản phẩm
      </div>
    );
  }

  const fmt = (n: number) => n.toLocaleString("vi-VN");

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-100">
            <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Sản phẩm</th>
            <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Data</th>
            <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Doanh thu</th>
            <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">% Ads</th>
            <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">CPA</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {products.map((p, i) => (
            <tr key={i} className="hover:bg-gray-50 transition-colors">
              <td className="py-3 px-4 font-medium text-gray-800">{p.productName}</td>
              <td className="py-3 px-4 text-right text-gray-700">{fmt(p.data)}</td>
              <td className="py-3 px-4 text-right text-gray-700">{fmt(p.revenue)} ₫</td>
              <td className="py-3 px-4 text-right">
                <span className={`font-medium ${p.adsPercentage > 30 ? "text-red-500" : "text-emerald-600"}`}>
                  {p.adsPercentage.toFixed(1)}%
                </span>
              </td>
              <td className="py-3 px-4 text-right text-gray-700">{fmt(p.costPerResult)} ₫</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
