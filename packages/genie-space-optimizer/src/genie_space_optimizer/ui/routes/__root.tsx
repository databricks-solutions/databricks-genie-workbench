import { ThemeProvider } from "@/components/apx/theme-provider";
import { useGetHealth } from "@/lib/api";
import NavbarUser from "@/components/navbar-user";
import { QueryClient } from "@tanstack/react-query";
import {
  createRootRouteWithContext,
  Link,
  Outlet,
  useRouterState,
} from "@tanstack/react-router";
import { Toaster } from "sonner";
import { AlertTriangle, BookOpen, ClipboardCopy, Settings, X } from "lucide-react";
import { useCallback, useState } from "react";

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: RootLayout,
});

/* ── Inline Setup Banner ─────────────────────────────────────────── */

function CopyBlock({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [text]);

  return (
    <div className="mt-2 flex items-start gap-2 rounded bg-amber-950/10 p-2 font-mono text-xs leading-relaxed">
      <pre className="flex-1 whitespace-pre-wrap break-all">{text}</pre>
      <button
        onClick={copy}
        className="flex shrink-0 items-center gap-1 rounded bg-amber-900/20 px-2 py-1 text-amber-800 hover:bg-amber-900/30"
      >
        <ClipboardCopy className="h-3 w-3" />
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

const DISMISS_KEY = "gso-health-dismissed";

function SetupBanner() {
  const { data, isError } = useGetHealth({
    query: {
      staleTime: 60_000,
      refetchInterval: 60_000,
      retry: 1,
    },
  });

  const [dismissed, setDismissed] = useState(
    () => sessionStorage.getItem(DISMISS_KEY) === "1",
  );

  if (dismissed || isError || !data || data.data.healthy) return null;

  const h = data.data;

  const issues: { msg: string; cmds: string[] }[] = [];
  if (h.message) {
    const cmds: string[] = [];
    if (h.createSchemaCommand) cmds.push(h.createSchemaCommand);
    if (h.grantCommand) cmds.push(h.grantCommand);
    issues.push({ msg: h.message, cmds });
  }
  if (h.jobMessage) {
    issues.push({ msg: h.jobMessage, cmds: [] });
  }

  if (issues.length === 0) return null;

  return (
    <div className="border-b border-amber-300 bg-amber-50 px-4 py-3 text-amber-900">
      <div className="mx-auto flex max-w-7xl items-start gap-3">
        <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-amber-600" />
        <div className="flex-1 space-y-3">
          <p className="text-sm font-semibold">Setup Required</p>
          {issues.map((issue, i) => (
            <div key={i} className="space-y-1">
              <p className="text-sm">{issue.msg}</p>
              {issue.cmds.map((cmd, j) => (
                <CopyBlock key={j} text={cmd} />
              ))}
            </div>
          ))}
          <p className="text-xs text-amber-700">
            After resolving the issue, re-deploy the app or refresh this page.{" "}
            <Link
              to="/settings"
              search={{ spaceId: undefined }}
              className="underline hover:text-amber-900"
            >
              See Settings for more details.
            </Link>
          </p>
        </div>
        <button
          onClick={() => {
            sessionStorage.setItem(DISMISS_KEY, "1");
            setDismissed(true);
          }}
          className="shrink-0 rounded p-1 text-amber-600 hover:bg-amber-200/50"
          aria-label="Dismiss"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

/* ── Root Layout ──────────────────────────────────────────────────── */

function RootLayout() {
  const pathname = useRouterState({ select: (s) => s.location.pathname });

  return (
    <ThemeProvider defaultTheme="light" storageKey="apx-ui-theme">
      <div className="flex min-h-screen flex-col bg-db-gray-bg">
        <header className="sticky top-0 z-50 bg-db-dark text-white shadow-md">
          <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4">
            <Link to="/" className="flex items-center gap-2.5 hover:opacity-90">
              <img
                src="/genie_logo.png"
                alt="Genie Optimizer"
                className="h-12 w-12 rounded-md object-cover"
              />
              <span className="text-lg font-semibold tracking-tight">
                Genie Space Optimizer
              </span>
            </Link>

            <div className="flex items-center gap-4">
              <nav className="flex items-center gap-1">
                <Link
                  to="/"
                  className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                    pathname === "/"
                      ? "bg-white/15 text-white"
                      : "text-white/70 hover:bg-white/10 hover:text-white"
                  }`}
                >
                  Dashboard
                </Link>
                <Link
                  to="/how-it-works"
                  className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                    pathname === "/how-it-works"
                      ? "bg-white/15 text-white"
                      : "text-white/70 hover:bg-white/10 hover:text-white"
                  }`}
                >
                  <BookOpen className="h-3.5 w-3.5" />
                  How It Works
                </Link>
                <Link
                  to="/settings"
                  search={{ spaceId: undefined }}
                  className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                    pathname === "/settings"
                      ? "bg-white/15 text-white"
                      : "text-white/70 hover:bg-white/10 hover:text-white"
                  }`}
                >
                  <Settings className="h-3.5 w-3.5" />
                  Settings
                </Link>
              </nav>

              <div className="h-5 w-px bg-white/20" />
              <NavbarUser />
            </div>
          </div>
        </header>

        <SetupBanner />

        <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-6">
          <Outlet />
        </main>

        <footer className="border-t border-db-gray-border bg-white py-4 text-center text-xs text-muted">
          &copy; {new Date().getFullYear()} Databricks &mdash; Genie Space
          Optimizer
        </footer>
      </div>
      <Toaster richColors />
    </ThemeProvider>
  );
}
