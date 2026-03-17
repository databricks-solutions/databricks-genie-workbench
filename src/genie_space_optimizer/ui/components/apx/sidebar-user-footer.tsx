import { Suspense, useMemo } from "react";
import { SidebarMenuButton } from "@/components/ui/sidebar";
import { useCurrentUserSuspense } from "@/lib/api";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Skeleton } from "@/components/ui/skeleton";
import selector from "@/lib/selector";

function SidebarUserFooterSkeleton() {
  return (
    <SidebarMenuButton size="lg">
      <Skeleton className="h-8 w-8 rounded-lg" />
      <div className="grid flex-1 text-left text-sm leading-tight gap-1">
        <Skeleton className="h-4 w-24 rounded" />
        <Skeleton className="h-3 w-46 rounded" />
      </div>
    </SidebarMenuButton>
  );
}

function SidebarUserFooterContent() {
  const { data: user } = useCurrentUserSuspense(selector());

  const firstLetters = useMemo(() => {
    const userName = user.user_name ?? "";
    const [first = "", ...rest] = userName.split(" ");
    const last = rest.at(-1) ?? "";
    return `${first[0] ?? ""}${last[0] ?? ""}`.toUpperCase();
  }, [user.user_name]);

  return (
    <SidebarMenuButton
      size="lg"
      className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
    >
      <Avatar className="h-8 w-8 rounded-lg grayscale">
        <AvatarFallback className="rounded-lg">{firstLetters}</AvatarFallback>
      </Avatar>
      <div className="grid flex-1 text-left text-sm leading-tight">
        <span className="truncate font-medium">{user.display_name}</span>
        <span className="text-muted truncate text-xs">
          {user.user_name}
        </span>
      </div>
    </SidebarMenuButton>
  );
}

export default function SidebarUserFooter() {
  return (
    <Suspense fallback={<SidebarUserFooterSkeleton />}>
      <SidebarUserFooterContent />
    </Suspense>
  );
}
