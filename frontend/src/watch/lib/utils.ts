// Watch-scoped `cn` re-export for self-contained imports inside watch pages.
// (Mirrors workbench's `@/lib/utils` shape, which the watch tree could call
// directly — keeping a local re-export keeps the watch folder excisable.)
export { cn } from '@/lib/utils'
