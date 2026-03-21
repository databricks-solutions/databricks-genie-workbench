# Genie Workbench UI Kit

This folder contains the complete visual identity of the Genie Workbench app,
packaged for adoption by the Genie Space Optimizer (GSO) standalone repo.

Copy this entire folder into your GSO repository, then follow the integration
instructions below to make the GSO visually consistent with the Workbench.

## What's Included

```
workbench-ui-kit/
├── README.md               ← You are here
├── styles/
│   └── index.css           ← Full theme: tokens, dark mode, gradients, animations, prose
├── components/
│   ├── accordion.tsx        ← AccordionItem (custom, no Radix)
│   ├── badge.tsx            ← Badge with semantic variants
│   ├── button.tsx           ← Button with gradient + lift effects
│   ├── card.tsx             ← Card with dark mode glow
│   ├── input.tsx            ← Input with accent focus ring
│   ├── progress.tsx         ← Progress bar with glow variant
│   ├── tabs.tsx             ← Tabs (context-based, no Radix)
│   └── textarea.tsx         ← Textarea
├── lib/
│   └── utils.ts             ← cn() helper + score color utilities
└── hooks/
    └── useTheme.ts          ← Dark mode toggle (system/light/dark)
```

## Integration Steps

### 1. Replace the theme stylesheet

Copy `styles/index.css` to replace your current GSO globals:

```
src/genie_space_optimizer/ui/styles/globals.css  ←  styles/index.css
```

### 2. Replace shared UI components

For each component that exists in BOTH the Workbench kit and the GSO,
replace the GSO version:

| Workbench kit file | GSO target path |
|-|-|
| `components/button.tsx` | `ui/components/ui/button.tsx` |
| `components/card.tsx` | `ui/components/ui/card.tsx` |
| `components/badge.tsx` | `ui/components/ui/badge.tsx` |
| `components/tabs.tsx` | `ui/components/ui/tabs.tsx` |
| `components/input.tsx` | `ui/components/ui/input.tsx` |
| `components/progress.tsx` | `ui/components/ui/progress.tsx` |
| `components/accordion.tsx` | `ui/components/ui/accordion.tsx` |
| `components/textarea.tsx` | `ui/components/ui/textarea.tsx` |

### 3. Keep GSO-only components but re-token them

The GSO has components the Workbench doesn't ship. Keep these files but
update their Tailwind classes using the token mapping below:

- `ui/components/ui/alert.tsx`
- `ui/components/ui/alert-dialog.tsx`
- `ui/components/ui/avatar.tsx`
- `ui/components/ui/chart.tsx`
- `ui/components/ui/checkbox.tsx`
- `ui/components/ui/collapsible.tsx`
- `ui/components/ui/dialog.tsx`
- `ui/components/ui/separator.tsx`
- `ui/components/ui/sheet.tsx`
- `ui/components/ui/sidebar.tsx`
- `ui/components/ui/skeleton.tsx`
- `ui/components/ui/table.tsx`
- `ui/components/ui/tooltip.tsx`

### 4. Replace utility and hook files

```
lib/utils.ts     →  ui/lib/utils.ts       (replace)
hooks/useTheme.ts →  ui/hooks/useTheme.ts  (add new file)
```

### 5. Install fonts

The Workbench uses custom fonts. Download and place .woff2 files in
`public/fonts/`:

- **Cabinet Grotesk** (Bold, Extrabold): https://www.fontshare.com/fonts/cabinet-grotesk
- **General Sans** (Regular, Medium, Semibold): https://www.fontshare.com/fonts/general-sans
- **JetBrains Mono** (Regular, Medium): https://www.jetbrains.com/lp/mono/

Expected files:
```
public/fonts/
├── CabinetGrotesk-Bold.woff2
├── CabinetGrotesk-Extrabold.woff2
├── GeneralSans-Regular.woff2
├── GeneralSans-Medium.woff2
├── GeneralSans-Semibold.woff2
├── JetBrainsMono-Regular.woff2
└── JetBrainsMono-Medium.woff2
```

If fonts are unavailable, the theme falls back to Inter → system-ui.

### 6. Wire up dark mode

Import and call `useTheme()` in your root layout component:

```tsx
import { useTheme } from "@/hooks/useTheme"

function RootLayout() {
  useTheme()  // applies dark class to <html> based on user preference
  return <>{/* ... */}</>
}
```

## Token Mapping Reference

When updating GSO-only components, use this mapping to translate shadcn/ui
default tokens to Workbench tokens:

### Backgrounds

| shadcn default | Workbench class | CSS variable |
|-|-|-|
| `bg-background` | `bg-primary` (via body) | `var(--bg-primary)` |
| `bg-card` | `bg-surface` | `var(--bg-surface)` |
| `bg-popover` | `bg-surface` | `var(--bg-surface)` |
| `bg-muted` | `bg-elevated` | `var(--bg-elevated)` |
| `bg-secondary` | `bg-elevated` | `var(--bg-elevated)` |
| `bg-accent` (shadcn hover) | `bg-elevated` | `var(--bg-elevated)` |

### Text

| shadcn default | Workbench class | CSS variable |
|-|-|-|
| `text-foreground` | `text-primary` | `var(--text-primary)` |
| `text-card-foreground` | `text-primary` | `var(--text-primary)` |
| `text-popover-foreground` | `text-primary` | `var(--text-primary)` |
| `text-muted-foreground` | `text-muted` | `var(--text-muted)` |
| `text-secondary-foreground` | `text-secondary` | `var(--text-secondary)` |
| `text-accent-foreground` | `text-primary` | `var(--text-primary)` |
| `text-primary-foreground` | `text-white` | `#FFFFFF` |
| `text-destructive-foreground` | `text-white` | `#FFFFFF` |
| `text-destructive` | `text-danger` | `var(--color-danger)` |

### Borders

| shadcn default | Workbench class | CSS variable |
|-|-|-|
| `border-border` | `border-default` | `var(--border-color)` |
| `border-input` | `border-default` | `var(--border-color)` |
| `ring-ring` | `ring-accent/50` | accent at 50% opacity |

### Primary Actions (buttons, highlights)

| shadcn default | Workbench equivalent |
|-|-|
| `bg-primary text-primary-foreground` | `gradient-accent text-white` (CSS class, not Tailwind utility) |
| `bg-destructive text-destructive-foreground` | `gradient-danger text-white` |
| `bg-primary/90` (hover) | `hover:shadow-xl hover:shadow-accent/30 hover:-translate-y-0.5` |

### Semantic Colors

The Workbench defines semantic colors as Tailwind theme extensions. Use them
directly:

| Purpose | Workbench class | Hex |
|-|-|-|
| Success | `text-success`, `bg-success/10` | `#10B981` |
| Warning | `text-warning`, `bg-warning/10` | `#F59E0B` |
| Danger | `text-danger`, `bg-danger/10` | `#EF4444` |
| Info | `text-info`, `bg-info/10` | `#3B82F6` |
| Accent | `text-accent`, `bg-accent/10` | `#4F46E5` |
| Cyan | `text-cyan`, `bg-cyan/10` | `#06B6D4` |

### Dark Mode

The Workbench uses a `.dark` class on `<html>`. Dark mode is handled via:

- CSS custom properties that swap in `.dark { ... }` block
- Tailwind `dark:` prefix utilities
- Special effects: `dark:card-glow`, `dark:glow-accent`

The `useTheme()` hook manages the toggle and persists to localStorage.

## Design Principles

- **Accent color**: Electric Indigo (`#4F46E5`) with lighter variant (`#818CF8`)
- **Secondary accent**: Cyan (`#06B6D4`) used for links and secondary highlights
- **Typography**: Cabinet Grotesk for headings, General Sans for body, JetBrains Mono for code
- **Border radius**: `rounded-lg` (0.75rem) for cards, `rounded-md` (0.5rem) for inputs
- **Shadows**: Subtle in light mode, glow effects in dark mode
- **Animations**: Fade-slide-up for staggered entry, scale-in for modals, pulse-glow for active states
- **Gradients**: 135deg gradients for primary actions (accent, success, warning, danger)

## Dependencies

Both the Workbench and this kit require:

- `tailwindcss` (v4+)
- `class-variance-authority` (CVA)
- `clsx`
- `tailwind-merge`
- `lucide-react` (for AccordionItem's ChevronDown icon)

The components do NOT depend on Radix UI. The Workbench's Tabs and
Accordion are custom implementations (context-based). If the GSO's existing
components use Radix (e.g., `@radix-ui/react-slot` in Button), you can
remove those dependencies after switching to the Workbench versions.
