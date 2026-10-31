import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AddisRide",
  description: "AddisRide is a ride-sharing app that connects riders with drivers in Addis Ababa, Ethiopia.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}