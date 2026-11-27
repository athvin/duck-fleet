/**
 * The console ships as a static export embedded in the `duckfleet` CLI binary
 * (server/cli/src/ui.rs). There is no Node server at runtime: every page is
 * prerendered HTML and all data comes from the gateway's `/v1/...` API, which
 * the CLI forwards with its own credential.
 *
 * @type {import('next').NextConfig}
 */
const nextConfig = {
  output: "export",
  reactStrictMode: true,
  poweredByHeader: false,
  images: { unoptimized: true },
};

export default nextConfig;
