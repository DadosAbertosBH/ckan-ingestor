import { expect, it } from 'vitest';
import { readCatalogResponse } from '../src/catalog-download';

it('reports catalog bytes as they stream from R2', async () => {
  const chunks = [Uint8Array.of(1, 2), Uint8Array.of(3, 4)];
  const response = new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  }), { headers: { 'content-length': '4' } });
  const progress: Array<{ received: number; total?: number }> = [];

  const bytes = await readCatalogResponse(response, state => progress.push(state));

  expect([...bytes]).toEqual([1, 2, 3, 4]);
  expect(progress).toEqual([
    { received: 2, total: 4 },
    { received: 4, total: 4 },
  ]);
});
