import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DuckFleet Console",
  description: "Live view of a DuckFleet gateway's pools and workers",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
