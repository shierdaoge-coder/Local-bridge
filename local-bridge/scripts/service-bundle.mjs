// Only executable service dependencies and usage notes belong in the install packages.
export const serviceBundleFiles = [
  'SHARED-SERVICE.md',
  'scripts/shared-service.mjs', 'scripts/daily-use-supervisor.mjs', 'scripts/relay-session.mjs',
  'src/service-start.mjs', 'src/dsh-service-start.mjs', 'src/entrypoint.mjs',
  'src/core.mjs', 'src/protocol.mjs', 'src/history.mjs', 'src/server.mjs', 'src/delivery.mjs',
  'src/session-pairs.mjs', 'src/native-session.mjs', 'src/client.mjs',
  'src/codex-gateway.mjs', 'src/dsh-gateway.mjs', 'src/session-tools.mjs', 'src/adapters/codex.mjs', 'src/adapters/dsh.mjs',
  'experimental/source-automation/bounded-reader.mjs', 'experimental/source-automation/candidate-core.mjs',
  'experimental/source-automation/candidate-service.mjs', 'experimental/source-automation/native-runtime.mjs',
  'experimental/source-automation/source-pump.mjs',
  'web/index.html', 'web/app.js', 'web/style.css',
];
