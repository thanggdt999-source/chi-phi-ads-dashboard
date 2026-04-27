"use client";

import { useState } from "react";
import { Button } from "@/components/ui/Button";

interface TelegramConnectProps {
  telegramUsername?: string;
  telegramId?: string;
  onConnected?: () => void;
  onDisconnected?: () => void;
}

export function TelegramConnect({
  telegramUsername,
  telegramId,
  onConnected,
  onDisconnected,
}: TelegramConnectProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [disconnecting, setDisconnecting] = useState(false);

  const isConnected = Boolean(telegramId);

  async function handleConnect() {
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/telegram/connect", { method: "POST" });
      const json = await res.json();
      if (!json.success) {
        setError(json.error ?? "Không thể tạo link kết nối");
        return;
      }
      // Open Telegram deep link in new tab
      window.open(json.data.deepLink, "_blank", "noopener,noreferrer");
      onConnected?.();
    } catch {
      setError("Lỗi kết nối server");
    } finally {
      setLoading(false);
    }
  }

  async function handleDisconnect() {
    setDisconnecting(true);
    try {
      await fetch("/api/telegram/connect", { method: "DELETE" });
      onDisconnected?.();
    } finally {
      setDisconnecting(false);
    }
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          {isConnected ? (
            <>
              <span className="w-2 h-2 rounded-full bg-emerald-500 flex-shrink-0" />
              <span className="text-sm text-gray-700">
                Connected:{" "}
                <span className="font-medium text-emerald-700">
                  {telegramUsername ? `@${telegramUsername}` : `ID ${telegramId}`}
                </span>
              </span>
            </>
          ) : (
            <>
              <span className="w-2 h-2 rounded-full bg-gray-300 flex-shrink-0" />
              <span className="text-sm text-gray-500">Chưa kết nối Telegram</span>
            </>
          )}
        </div>

        <div className="flex items-center gap-2">
          {isConnected ? (
            <Button
              variant="ghost"
              size="sm"
              loading={disconnecting}
              onClick={handleDisconnect}
              className="text-red-500 hover:text-red-700 hover:bg-red-50"
            >
              Ngắt kết nối
            </Button>
          ) : (
            <Button
              variant="secondary"
              size="sm"
              loading={loading}
              onClick={handleConnect}
              className="gap-1.5"
            >
              <TelegramIcon />
              Connect Telegram
            </Button>
          )}
        </div>
      </div>

      {error && <p className="text-xs text-red-500">{error}</p>}

      {!isConnected && (
        <p className="text-xs text-gray-400">
          Nhấn &ldquo;Connect Telegram&rdquo; → link sẽ mở Telegram Bot → nhấn START để hoàn tất liên kết tự động.
        </p>
      )}
    </div>
  );
}

function TelegramIcon() {
  return (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
      <path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.562 8.248-2.012 9.483c-.148.658-.537.818-1.088.508l-3-2.21-1.447 1.392c-.16.16-.294.294-.603.294l.215-3.048 5.546-5.01c.241-.215-.053-.334-.373-.12L7.51 14.697l-2.95-.924c-.642-.2-.655-.642.133-.95l11.52-4.44c.533-.193 1.003.13.35.865z" />
    </svg>
  );
}
