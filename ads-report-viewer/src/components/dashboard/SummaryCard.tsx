import { Card } from "@/components/ui/Card";

interface SummaryCardProps {
  label: string;
  value: string | number;
  subValue?: string;
  icon: string;
  color?: "blue" | "green" | "amber" | "purple";
}

const colorMap = {
  blue: "bg-blue-50 text-blue-600",
  green: "bg-emerald-50 text-emerald-600",
  amber: "bg-amber-50 text-amber-600",
  purple: "bg-violet-50 text-violet-600",
};

export function SummaryCard({ label, value, subValue, icon, color = "blue" }: SummaryCardProps) {
  return (
    <Card className="flex items-start gap-4">
      <div className={`w-11 h-11 rounded-xl flex items-center justify-center text-xl flex-shrink-0 ${colorMap[color]}`}>
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</p>
        <p className="text-xl font-semibold text-gray-900 mt-0.5 truncate">{value}</p>
        {subValue && <p className="text-xs text-gray-400 mt-0.5">{subValue}</p>}
      </div>
    </Card>
  );
}
