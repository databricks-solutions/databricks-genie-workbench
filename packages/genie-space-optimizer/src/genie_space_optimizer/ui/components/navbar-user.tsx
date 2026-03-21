import { Suspense, useMemo } from "react";
import { useCurrentUserSuspense } from "@/lib/api";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import selector from "@/lib/selector";
import { User } from "lucide-react";

function NavbarUserSkeleton() {
  return (
    <div className="flex items-center gap-2">
      <div className="h-7 w-7 animate-pulse rounded-full bg-white/20" />
      <div className="hidden sm:block h-3 w-20 animate-pulse rounded bg-white/20" />
    </div>
  );
}

function NavbarUserContent() {
  const { data: user } = useCurrentUserSuspense(selector());

  const initials = useMemo(() => {
    const name = user.display_name ?? user.user_name ?? "";
    const parts = name.split(/[\s@.]+/).filter(Boolean);
    if (parts.length >= 2) {
      return `${parts[0][0]}${parts[1][0]}`.toUpperCase();
    }
    return name.slice(0, 2).toUpperCase() || "?";
  }, [user.display_name, user.user_name]);

  const displayName = user.display_name ?? user.user_name?.split("@")[0] ?? "";

  return (
    <div className="flex items-center gap-2">
      <Avatar className="h-7 w-7 border border-white/30">
        <AvatarFallback className="bg-white/15 text-xs font-medium text-white">
          {initials}
        </AvatarFallback>
      </Avatar>
      <span className="hidden text-sm font-medium text-white/90 sm:block">
        {displayName}
      </span>
    </div>
  );
}

function NavbarUserFallback() {
  return (
    <div className="flex items-center gap-2 text-white/50">
      <User className="h-5 w-5" />
    </div>
  );
}

export default function NavbarUser() {
  return (
    <Suspense fallback={<NavbarUserSkeleton />}>
      <NavbarUserErrorBoundary>
        <NavbarUserContent />
      </NavbarUserErrorBoundary>
    </Suspense>
  );
}

import { Component, type ReactNode } from "react";

class NavbarUserErrorBoundary extends Component<
  { children: ReactNode },
  { hasError: boolean }
> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  render() {
    if (this.state.hasError) {
      return <NavbarUserFallback />;
    }
    return this.props.children;
  }
}
