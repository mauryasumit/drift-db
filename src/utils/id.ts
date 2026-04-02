import { v4 as uuidv4 } from 'uuid';
import { randomBytes } from 'crypto';

export function generateId(): string {
  return uuidv4();
}

export function generateNodeId(): string {
  return randomBytes(6).toString('hex');
}
