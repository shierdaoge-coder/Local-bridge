import { mkdir, readFile, writeFile, copyFile } from 'node:fs/promises';
import { resolve, dirname, join } from 'node:path';
import assert from 'node:assert/strict';
import { digest } from '../../src/protocol.mjs';
import { parseArgs } from 'node:util';

const project = resolve(import.meta.dirname, '../..');
const { values } = parseArgs({ options: { output: { type: 'string' } } });
const output = resolve(values.output ?? resolve(project, 'dist', 'service'));
const files = [];
const importPattern = /^\s*import\s+(?:[^;]*?\sfrom\s*)?['"]([^'"]+)['"]/gm;
async function visit(file) {
  if (files.includes(file)) return;
  assert.ok(file.startsWith('src/') || file.startsWith('experimental/source-automation/'));
  files.push(file);
  const text = await readFile(resolve(project, file), 'utf8');
  for (const match of text.matchAll(importPattern)) {
    if (!match[1].startsWith('.')) { assert.ok(match[1].startsWith('node:')); continue; }
    const target = resolve(dirname(resolve(project, file)), match[1]);
    assert.ok(target.startsWith(project + '\\') || target.startsWith(project + '/'));
    const relative = target.slice(project.length + 1).replaceAll('\\', '/'); await visit(relative);
  }
}
await visit('experimental/source-automation/candidate-main.mjs');
files.push(...['index.html', 'app.js', 'style.css'].map(n => `web/${n}`));
await mkdir(dirname(output), { recursive: true });
await mkdir(output);
const manifest = [];
for (const file of files) {
  const target = resolve(output, file); assert.ok(target.startsWith(output + '\\') || target.startsWith(output + '/'));
  await mkdir(dirname(target), { recursive: true }); await copyFile(resolve(project, file), target);
  const bytes = await readFile(target); assert.equal(digest(bytes), digest(await readFile(resolve(project, file))));
  manifest.push({ path: file, bytes: bytes.length, sha256: digest(bytes) });
  if (file.endsWith('.mjs')) for (const match of bytes.toString('utf8').matchAll(/^\s*import\s+(?:[^;]*?\sfrom\s*)?['"]([^'"]+)['"]/gm)) {
    if (!match[1].startsWith('.')) { assert.ok(match[1].startsWith('node:')); continue; }
    const dependency = resolve(dirname(target), match[1]);
    assert.ok(files.some(f => resolve(output, f) === dependency), 'Runtime dependency must be in the exact candidate allowlist');
  }
}
const readme = `# local-bridge service

Run from the service bundle root with Node 24.

    node experimental/source-automation/candidate-main.mjs --config "ABSOLUTE_PRIVATE_CONFIG_PATH"

Replace ABSOLUTE_PRIVATE_CONFIG_PATH with the absolute path to the private service JSON file. Add a directory field to that file and replace ABSOLUTE_SERVICE_DATA_DIRECTORY with an absolute path to a private data directory on the target machine.

    { "directory": "ABSOLUTE_SERVICE_DATA_DIRECTORY" }

Keep the configuration, database and source archives private. Use the full source tree to run the supervisor or enable DSH automatic startup.
`;
await writeFile(join(output, 'README.md'), readme);
manifest.push({ path: 'README.md', bytes: Buffer.byteLength(readme), sha256: digest(readme) });
await writeFile(join(output, 'MANIFEST.json'), JSON.stringify({ kind: 'local-bridge-service', files: manifest }, null, 2) + '\n');
console.log(output);
