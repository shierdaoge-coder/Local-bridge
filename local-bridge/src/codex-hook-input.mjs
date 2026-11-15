import { StringDecoder } from 'node:string_decoder';
import { insist } from './protocol.mjs';

/** One host event per invocation. A complete JSON event does not require pipe EOF. */
export async function readHookInput(input, { maxBytes = 1_048_576 } = {}) {
  const decoder = new StringDecoder('utf8'); let bytes = 0, json = '';
  for await (const chunk of input) {
    const part = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += part.length; insist(bytes <= maxBytes, 'PAYLOAD_TOO_LARGE', 'Hook input exceeds local limit');
    json += decoder.write(part);
    try { return JSON.parse(json.replace(/^\uFEFF/, '')); }
    catch (error) { if (!(error instanceof SyntaxError)) throw error; }
  }
  json += decoder.end();
  return JSON.parse(json.replace(/^\uFEFF/, ''));
}
