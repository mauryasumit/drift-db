import { gzip, gunzip, constants } from 'zlib';
import { promisify } from 'util';

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

export async function compress(data: Buffer | string): Promise<Buffer> {
  const buf = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
  return gzipAsync(buf, { level: constants.Z_BEST_SPEED });
}

export async function decompress(data: Buffer): Promise<Buffer> {
  return gunzipAsync(data);
}
