import { realpathSync } from 'node:fs';
import { resolve, win32 } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Return whether argv[1] names this module, following filesystem aliases. */
export function isMainModule(moduleUrl, { argv1 = process.argv[1] } = {}) {
  if (typeof argv1 !== 'string' || !argv1) return false;
  try {
    const entry = realpathSync.native(resolve(argv1));
    const module = realpathSync.native(fileURLToPath(moduleUrl));
    return process.platform === 'win32'
      ? win32.normalize(entry).toLowerCase() === win32.normalize(module).toLowerCase()
      : entry === module;
  } catch {
    return false;
  }
}
