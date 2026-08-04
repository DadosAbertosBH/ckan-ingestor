import type { Job } from "@/types";

export interface Duration {
  ms: number;
  finished: boolean;
}

function parseAsUTC(dt: string): Date {
  return new Date(dt);
}

export function jobDuration(job: Job, now: Date = new Date()): Duration | null {
  if (!job.started_at) return null;

  const start = parseAsUTC(job.started_at).getTime();
  // Only use completed_at if the job is actually finished — a stale
  // completed_at from a previous run must be ignored for active jobs.
  const isFinished = job.status === "completed" || job.status === "failed";
  const end =
    isFinished && job.completed_at
      ? parseAsUTC(job.completed_at).getTime()
      : now.getTime();

  return { ms: Math.max(0, end - start), finished: isFinished };
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
