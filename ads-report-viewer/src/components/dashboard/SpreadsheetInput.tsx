"use client";

import { useState, useRef } from "react";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";

interface SpreadsheetInputProps {
  defaultUrl?: string;
  urlHistory?: string[];
  onSubmit: (url: string) => Promise<void>;
  loading?: boolean;
}

export function SpreadsheetInput({ defaultUrl = "", urlHistory = [], onSubmit, loading }: SpreadsheetInputProps) {
  const [url, setUrl] = useState(defaultUrl);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const suggestions = urlHistory.filter(
    (u) => u !== url && u.toLowerCase().includes(url.toLowerCase())
  );

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!url.trim()) return;
    await onSubmit(url.trim());
    setShowSuggestions(false);
  }

  return (
    <form onSubmit={handleSubmit} className="relative">
      <div className="flex gap-2">
        <div className="flex-1 relative">
          <Input
            ref={inputRef}
            type="url"
            value={url}
            onChange={(e) => {
              setUrl(e.target.value);
              setShowSuggestions(true);
            }}
            onFocus={() => setShowSuggestions(true)}
            onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
            placeholder="https://docs.google.com/spreadsheets/d/..."
            required
            className="pr-4"
          />
          {showSuggestions && suggestions.length > 0 && (
            <ul className="absolute z-10 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto">
              {suggestions.map((s, i) => (
                <li
                  key={i}
                  className="px-3 py-2.5 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer truncate"
                  onMouseDown={() => {
                    setUrl(s);
                    setShowSuggestions(false);
                  }}
                >
                  {s}
                </li>
              ))}
            </ul>
          )}
        </div>
        <Button type="submit" loading={loading} disabled={!url.trim()}>
          Đọc dữ liệu
        </Button>
      </div>
      <p className="mt-2 text-xs text-gray-400">
        Dán link Google Sheet của bạn. Hệ thống sẽ tự động phân tích báo cáo chi phí Ads.
      </p>
    </form>
  );
}
