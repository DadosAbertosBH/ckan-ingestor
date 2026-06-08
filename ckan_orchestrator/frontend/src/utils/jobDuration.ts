import type { Job } from "@/types";

export interface Duration {
  ms: number;
  finished: boolean;
}

function parseAsUTC(dt: string): Date {
  // API returns datetimes without timezone (e.g. "2025-06-08T19:50:00").
  // Append Z so JS parses as UTC instead of local time.
  if (dt.endsWith("Z") || /[+-]\d{2}:?\d{2}$/.test(dt)) {
    return new Date(dt);
  }
  return new Date(dt + "Z");
}

export function jobDuration(job: Job, now: Date = new Date()): Duration | null {
  if (!job.started_at) return null;

  const start = parseAsUTC(job.started_at).getTime();
  const end = job.completed_at
    ? parseAsUTC(job.completed_at).getTime()
    : now.getTime();

  return { ms: end - start, finished: job.completed_at != null };
}

export function formatDuration(ms: number): string {
  const totalSeconds = Math.floor(ms / 1000);
  if (totalSeconds <= 0) return "0s";

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);

  return parts.join(" ");
}
