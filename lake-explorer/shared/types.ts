export interface Column { name: string; type: string }
export interface LakeTable {
  schema: string; name: string; title: string; columns: Column[]; rows: string;
  description?: string; source?: string; dataset?: string;
}
export interface Publication {
  id: string; previousId?: string; createdAt: string; snapshotId: string; catalogKey: string;
  protectedFilesKey: string; sha256: string; size: number; bucket: string;
  duckdbVersion: string; ducklakeVersion: string; formatVersion: string; tables: LakeTable[]; dataPath?: string;
}
export interface Credentials { accessKeyId: string; secretAccessKey: string; sessionToken: string }
export interface Session { publication: Publication; endpoint: string; credentials: Credentials; expiresAt: number; files?: string[] }
export interface QueryResult { columns: Column[]; rows: Record<string, unknown>[]; truncated: boolean; elapsedMs: number }
