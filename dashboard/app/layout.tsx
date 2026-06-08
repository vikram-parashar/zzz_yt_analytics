import type { Metadata } from "next";
import "./globals.css";
export const metadata: Metadata = {
  title: "ZZZ YT Analytics",
  description: "Zenless Zone Zero YouTube Analytics Dashboard",
};
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="catppuccin" suppressHydrationWarning>
      <body className="antialiased">{children}</body>
    </html>
  );
}