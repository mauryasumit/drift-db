import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
  type S3ClientConfig,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { Readable } from 'stream';
import type { S3Config, SyncManifest } from '../types.js';
import { compress, decompress } from '../utils/compress.js';
import { encrypt, decrypt } from '../utils/crypto.js';

export interface S3UploadOptions {
  compress?: boolean;
  encryptionKey?: string;
}

export class S3Adapter {
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(config: S3Config) {
    const clientConfig: S3ClientConfig = {
      region: config.region,
    };

    if (config.accessKeyId && config.secretAccessKey) {
      clientConfig.credentials = {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      };
    }

    if (config.endpoint) {
      clientConfig.endpoint = config.endpoint;
      clientConfig.forcePathStyle = config.forcePathStyle ?? true;
    }

    this.client = new S3Client(clientConfig);
    this.bucket = config.bucket;
    this.prefix = config.prefix ? config.prefix.replace(/\/$/, '') : 'driftdb';
  }

  private key(path: string): string {
    return `${this.prefix}/${path}`;
  }

  async upload(
    path: string,
    data: Buffer,
    options: S3UploadOptions = {}
  ): Promise<void> {
    let payload = data;

    if (options.compress) {
      payload = await compress(payload);
    }

    if (options.encryptionKey) {
      payload = encrypt(payload, options.encryptionKey);
    }

    const contentEncoding = options.compress ? 'gzip' : undefined;
    const metadata: Record<string, string> = {};
    if (options.compress) metadata['x-driftdb-compressed'] = '1';
    if (options.encryptionKey) metadata['x-driftdb-encrypted'] = '1';

    if (payload.length > 5 * 1024 * 1024) {
      const upload = new Upload({
        client: this.client,
        params: {
          Bucket: this.bucket,
          Key: this.key(path),
          Body: Readable.from(payload),
          ContentEncoding: contentEncoding,
          Metadata: metadata,
        },
      });
      await upload.done();
    } else {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.key(path),
          Body: payload,
          ContentEncoding: contentEncoding,
          Metadata: metadata,
        })
      );
    }
  }

  async download(
    path: string,
    options: S3UploadOptions = {}
  ): Promise<Buffer> {
    const response = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: this.key(path) })
    );

    if (!response.Body) {
      throw new Error(`Empty response body for key: ${path}`);
    }

    const chunks: Buffer[] = [];
    for await (const chunk of response.Body as AsyncIterable<Uint8Array>) {
      chunks.push(Buffer.from(chunk));
    }
    let data = Buffer.concat(chunks);

    if (options.encryptionKey) {
      data = Buffer.from(decrypt(data, options.encryptionKey));
    }

    if (options.compress) {
      data = Buffer.from(await decompress(data));
    }

    return data;
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: this.key(path) })
      );
      return true;
    } catch {
      return false;
    }
  }

  async listKeys(prefix: string): Promise<string[]> {
    const fullPrefix = this.key(prefix);
    const keys: string[] = [];
    let continuationToken: string | undefined;

    do {
      const response = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: fullPrefix,
          ContinuationToken: continuationToken,
        })
      );

      for (const obj of response.Contents ?? []) {
        if (obj.Key) {
          keys.push(obj.Key.slice(this.prefix.length + 1));
        }
      }

      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return keys;
  }

  async putManifest(dbName: string, manifest: SyncManifest): Promise<void> {
    const data = Buffer.from(JSON.stringify(manifest), 'utf8');
    await this.upload(`databases/${dbName}/manifest.json`, data);
  }

  async getManifest(dbName: string): Promise<SyncManifest | null> {
    const path = `databases/${dbName}/manifest.json`;
    if (!(await this.exists(path))) return null;
    const data = await this.download(path);
    return JSON.parse(data.toString('utf8')) as SyncManifest;
  }

  logKey(dbName: string, nodeId: string, fromSeq: number, toSeq: number): string {
    return `databases/${dbName}/nodes/${nodeId}/logs/${String(fromSeq).padStart(12, '0')}-${String(toSeq).padStart(12, '0')}.json`;
  }

  snapshotKey(dbName: string, nodeId: string, timestamp: number): string {
    return `databases/${dbName}/nodes/${nodeId}/snapshots/${timestamp}.sqlite`;
  }
}
