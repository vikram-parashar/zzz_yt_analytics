"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Users,
  TrendingUp,
  Tv,
  Activity,
} from "lucide-react";

const NAV_ITEMS = [
  { href: "/", label: "Overview", icon: LayoutDashboard },
  { href: "/agents", label: "Agents", icon: Users },
  { href: "/trends", label: "Trends", icon: TrendingUp },
  { href: "/channels", label: "Channels", icon: Tv },
  { href: "/runs", label: "Pipeline Runs", icon: Activity },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-64 min-h-screen bg-base-200 border-r border-base-300 flex flex-col">
      <div className="p-4 border-b border-base-300">
        <Link href="/" className="flex items-center gap-2">
          <span className="text-2xl">🎮</span>
          <div>
            <h1 className="text-lg font-bold leading-tight">ZZZ Analytics</h1>
            <p className="text-xs opacity-60">Character Popularity Tracker</p>
          </div>
        </Link>
      </div>

      <nav className="flex-1 p-2">
        <ul className="menu menu-md gap-1">
          {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
            const active = pathname === href;
            return (
              <li key={href}>
                <Link
                  href={href}
                  className={active ? "active" : ""}
                >
                  <Icon size={18} />
                  {label}
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>

      <div className="p-4 border-t border-base-300">
        <p className="text-xs opacity-40">Zenless Zone Zero</p>
        <p className="text-xs opacity-40">YouTube Analytics v0.2</p>
      </div>
    </aside>
  );
}

