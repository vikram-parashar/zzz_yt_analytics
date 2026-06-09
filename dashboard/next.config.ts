import type { NextConfig } from "next";
const nextConfig: NextConfig = {
  images: {
    remotePatterns: [new URL('https://lblog.fp.guinfra.com/**'),new URL('https://yt3.ggpht.com/**')],
  },
  serverExternalPackages: ['@duckdb/node-api', '@duckdb/node-bindings'],
  output: 'standalone',
  typescript: {
    ignoreBuildErrors: true,
  },
};
export default nextConfig;