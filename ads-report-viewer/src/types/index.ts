// Shared TypeScript types across the app

export type Role = "viewer" | "leader" | "admin";

export interface UserSession {
  userId: string;
  username: string;
  role: Role;
  team: string;
  displayName?: string;
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
}

// ── Report types ──────────────────────────────────────────

export interface ProductMetric {
  productName: string;
  data: number;
  revenue: number;
  adsPercentage: number;
  costPerResult: number;
}

export interface ReportSummary {
  date: string;
  totalData: number;
  totalRevenue: number;
  adsPercentage: number;
  costPerResult: number;
  products: ProductMetric[];
}

// ── Google Sheets parsed data ─────────────────────────────

export interface SheetReport {
  owner: string;
  team: string;
  summary: {
    totalData: number;
    totalRevenue: number;
    adsPercentage: number;
    costPerResult: number;
  };
  products: ProductMetric[];
}

// ── User ──────────────────────────────────────────────────

export interface PublicUser {
  id: string;
  username: string;
  displayName: string;
  team: string;
  role: Role;
  telegramId: string;
  telegramUsername: string;
  createdAt: string;
}
