import { readFile, writeFile, mkdir, rename, realpath, lstat, symlink, unlink, rmdir, open } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { DSH_PACKAGE } from './build-dsh.mjs';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const same = (a, b) => process.platform === 'win32' ? path.resolve(a).toLowerCase() === path.resolve(b).toLowerCase() : path.resolve(a) === path.resolve(b);
const within = (p, root) => { const rel = path.relative(root, p); return rel === '' || (rel !== '..' && !rel.startsWith('..' + path.sep) && !path.isAbsolute(rel)); };
async function optional(file) { try { return await readFile(file, 'utf8'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } }
async function atomic(file, text) { const temp = file + '.bridge-' + randomUUID(); try { await writeFile(temp, text, { flag: 'wx' }); await rename(temp, file); } finally { await unlink(temp).catch(e => { if (e.code !== 'ENOENT') throw e; }); } }

/** No package manager, model call or restart. Dry unless the operator passes --write. */
export async function installDsh({ source, profile, write = false, backupRoot = path.join(project, '.runtime', 'dsh-install-backups') }) {
  if (!source || !profile || !path.isAbsolute(source) || !path.isAbsolute(profile)) throw Error('Exact absolute source and existing profile directories required');
  source = await realpath(source); profile = await realpath(profile);
  const sourceManifest = JSON.parse(await readFile(path.join(source, 'package.json'), 'utf8'));
  if (sourceManifest.name !== DSH_PACKAGE || sourceManifest.dsh?.bundle?.patch !== './cordis.patch.yml') throw Error('Source is not the built DSH bridge bundle');
  await readFile(path.join(source, 'src/dsh-plugin.mjs'));
  const manifestFile = path.join(profile, 'package.json'), lockFile = path.join(profile, 'pnpm-lock.yaml');
  const before = await readFile(manifestFile, 'utf8'), manifest = JSON.parse(before), lockBefore = await optional(lockFile);
  if (!Array.isArray(manifest.dsh?.profile?.bundles)) throw Error('Target is not an initialized DSH profile');
  const target = path.join(profile, 'node_modules', '@local', 'local-bridge');
  for (const dir of [path.join(profile, 'node_modules'), path.dirname(target)]) {
    try { if (!within(await realpath(dir), profile)) throw Error('Profile dependency directory points outside the selected profile'); }
    catch (e) { if (e.code !== 'ENOENT') throw e; }
  }
  let linked = false;
  try { await lstat(target); if (!same(await realpath(target), source)) throw Error('Existing bridge package has a different source; use DSH plugin management to remove it first'); linked = true; }
  catch (e) { if (e.code !== 'ENOENT') throw e; }
  const specifier = 'link:' + source.replaceAll('\\', '/');
  if (manifest.dependencies?.[DSH_PACKAGE] && manifest.dependencies[DSH_PACKAGE] !== specifier) throw Error('Existing dependency registration differs; no files changed');
  let lockAfter = lockBefore;
  if (lockBefore !== null) {
    const importer = /(importers:\r?\n(?:\r?\n)*  \.:\r?\n    dependencies:\r?\n)/;
    if (!importer.test(lockBefore)) throw Error('Unrecognized profile lockfile; no files changed');
    const entry = /^      ['"]?@local\/local-bridge['"]?:\s*\r?\n        specifier: (.+)\r?\n        version: (.+)$/m.exec(lockBefore);
    const version = 'link:' + path.relative(profile, source).replaceAll('\\', '/');
    if (entry && (entry[1] !== specifier || entry[2] !== version)) throw Error('Existing lock entry differs; no files changed');
    if (!entry) {
      if (lockBefore.includes(DSH_PACKAGE)) throw Error('Ambiguous lockfile reference; no files changed');
      const nl = lockBefore.includes('\r\n') ? '\r\n' : '\n';
      lockAfter = lockBefore.replace(importer, '$1' + [`      '${DSH_PACKAGE}':`, `        specifier: ${specifier}`, `        version: ${version}`, ''].join(nl));
    }
  }
  const bundles = manifest.dsh.profile.bundles.includes(DSH_PACKAGE) ? manifest.dsh.profile.bundles : [...manifest.dsh.profile.bundles, DSH_PACKAGE];
  const after = JSON.stringify({ ...manifest, dependencies: { ...manifest.dependencies, [DSH_PACKAGE]: specifier }, dsh: { ...manifest.dsh, profile: { ...manifest.dsh.profile, bundles } } }, null, 2) + '\n';
  const plan = { writes: write, source, profile, target, package: DSH_PACKAGE, existing_link: linked, profile_reload_required: true, changes: ['local dependency link', 'dependency and bundle registration', ...(lockBefore === null ? [] : ['root lockfile dependency'])] };
  if (!write) return plan;
  const guardPath = path.join(profile, '.local-bridge-install.lock'), guard = await open(guardPath, 'wx', 0o600);
  const backup = path.join(path.resolve(backupRoot), randomUUID()); let created = false, mutated = false;
  try {
    if (await readFile(manifestFile, 'utf8') !== before || await optional(lockFile) !== lockBefore) throw Error('Profile changed during preparation; retry after inspection');
    await mkdir(backup, { recursive: true });
    await writeFile(path.join(backup, 'package.json'), before, { flag: 'wx', mode: 0o600 });
    if (lockBefore !== null) await writeFile(path.join(backup, 'pnpm-lock.yaml'), lockBefore, { flag: 'wx', mode: 0o600 });
    await writeFile(path.join(backup, 'install.json'), JSON.stringify({ source, profile, target, created_link: !linked }, null, 2) + '\n', { flag: 'wx' });
    if (!linked) { await mkdir(path.dirname(target), { recursive: true }); await symlink(source, target, process.platform === 'win32' ? 'junction' : 'dir'); created = true; }
    mutated = true;
    if (lockAfter !== lockBefore) await atomic(lockFile, lockAfter);
    await atomic(manifestFile, after);
    return { ...plan, backup, installed: true };
  } catch (error) {
    if (mutated) { await atomic(manifestFile, before); if (lockBefore !== null) await atomic(lockFile, lockBefore); }
    if (created && same(await realpath(target), source)) { if (process.platform === 'win32') await rmdir(target); else await unlink(target); }
    throw error;
  } finally { await guard.close(); await unlink(guardPath); }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const flags = { '--source': 'source', '--profile-dir': 'profile' }, options = {}, argv = process.argv.slice(2);
    if (argv.includes('--help')) process.stdout.write('DSH local bundle installer (dry by default): --source ABS_BUNDLE --profile-dir ABS_EXISTING_PROFILE [--write]\n');
    else {
      for (let i = 0; i < argv.length; i++) { if (argv[i] === '--write') options.write = true; else { if (!flags[argv[i]] || !argv[i + 1] || argv[i + 1].startsWith('--')) throw Error('Unknown or incomplete installer option'); options[flags[argv[i]]] = argv[++i]; } }
      process.stdout.write(JSON.stringify(await installDsh(options), null, 2) + '\n');
    }
  } catch (e) { process.stderr.write(e.message + '\n'); process.exitCode = 1; }
}
