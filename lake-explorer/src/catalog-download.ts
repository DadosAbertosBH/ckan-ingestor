export interface DownloadProgress {
  received: number;
  total?: number;
}

export async function readCatalogResponse(
  response: Response,
  onProgress: (progress: DownloadProgress) => void,
): Promise<Uint8Array> {
  const contentLength = Number(response.headers.get('content-length'));
  const total = Number.isFinite(contentLength) && contentLength > 0 ? contentLength : undefined;

  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    onProgress({ received: bytes.byteLength, total });
    return bytes;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let received = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.byteLength;
    onProgress({ received, total });
  }

  const bytes = new Uint8Array(received);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}
