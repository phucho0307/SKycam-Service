import { useEffect, useRef } from "react";

declare global {
  interface Window {
    turnstile?: {
      render: (
        el: HTMLElement | string,
        opts: {
          sitekey: string;
          callback?: (token: string) => void;
          "error-callback"?: () => void;
          "expired-callback"?: () => void;
          theme?: "light" | "dark" | "auto";
        }
      ) => string;
      reset: (id: string) => void;
      remove: (id: string) => void;
    };
  }
}

const SCRIPT_SRC =
  "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit";

function ensureScriptLoaded(): Promise<void> {
  return new Promise((resolve) => {
    if (window.turnstile) return resolve();
    const existing = document.querySelector<HTMLScriptElement>(
      `script[src^="https://challenges.cloudflare.com/turnstile"]`
    );
    if (existing) {
      const check = setInterval(() => {
        if (window.turnstile) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      return;
    }
    const s = document.createElement("script");
    s.src = SCRIPT_SRC;
    s.async = true;
    s.defer = true;
    s.onload = () => {
      const check = setInterval(() => {
        if (window.turnstile) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    };
    document.head.appendChild(s);
  });
}

interface Props {
  sitekey: string;
  onToken: (token: string | null) => void;
}

export default function Turnstile({ sitekey, onToken }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const widgetIdRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    ensureScriptLoaded().then(() => {
      if (cancelled || !containerRef.current || !window.turnstile) return;
      widgetIdRef.current = window.turnstile.render(containerRef.current, {
        sitekey,
        theme: "dark",
        callback: (token) => onToken(token),
        "error-callback": () => onToken(null),
        "expired-callback": () => onToken(null),
      });
    });
    return () => {
      cancelled = true;
      if (widgetIdRef.current && window.turnstile) {
        try {
          window.turnstile.remove(widgetIdRef.current);
        } catch {
          /* widget may already be gone */
        }
        widgetIdRef.current = null;
      }
    };
  }, [sitekey, onToken]);

  return <div ref={containerRef} />;
}
