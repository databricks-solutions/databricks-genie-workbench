import { createContext, useContext } from "react";
import {
  useTheme as useThemeHook,
  type Theme,
} from "@/hooks/useTheme";

type ThemeProviderProps = {
  children: React.ReactNode;
  defaultTheme?: Theme;
  storageKey?: string;
};

type ThemeProviderState = {
  theme: Theme;
  resolvedTheme: "light" | "dark";
  setTheme: (theme: Theme) => void;
  toggleTheme: () => void;
  toggleLightDark: () => void;
  isDark: boolean;
  isLight: boolean;
  isSystem: boolean;
};

const initialState: ThemeProviderState = {
  theme: "system",
  resolvedTheme: "light",
  setTheme: () => null,
  toggleTheme: () => null,
  toggleLightDark: () => null,
  isDark: false,
  isLight: true,
  isSystem: true,
};

const ThemeProviderContext = createContext<ThemeProviderState>(initialState);

export function ThemeProvider({
  children,
}: ThemeProviderProps) {
  const themeState = useThemeHook();

  return (
    <ThemeProviderContext.Provider value={themeState}>
      {children}
    </ThemeProviderContext.Provider>
  );
}

export const useTheme = () => {
  const context = useContext(ThemeProviderContext);

  if (context === undefined)
    throw new Error("useTheme must be used within a ThemeProvider");

  return context;
};
