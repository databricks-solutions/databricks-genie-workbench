import { Link } from "@tanstack/react-router";

interface LogoProps {
  to?: string;
  className?: string;
  showText?: boolean;
}

export function Logo({ to = "/", className = "", showText = true }: LogoProps) {
  const content = (
    <div className={`flex items-center gap-2 ${className}`}>
      <img
        src="/logo.svg"
        alt="logo"
        className="h-6 w-6 text-primary border border-primary rounded-sm"
      />
      {showText && (
        <span className="font-semibold text-lg">{__APP_NAME__}</span>
      )}
    </div>
  );

  if (to) {
    return (
      <Link to={to} className="hover:opacity-80 transition-opacity">
        {content}
      </Link>
    );
  }

  return content;
}

export default Logo;
