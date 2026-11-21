import {
  ERROR,
  MemorySaver,
  emptyCheckpoint,
  uuid6,
  type Checkpoint,
  type CheckpointMetadata,
  type SerializerProtocol,
} from '@langchain/langgraph-checkpoint';
import type { RunnableConfig } from '@langchain/core/runnables';
import { evictDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { DurableObjectSaver } from '../src/index';
import { collect, inObject, inStorage, newObject } from './helpers';

// LangChain's default serializer. JsonPlusSerializer itself is only exported
// from @langchain/langgraph-checkpoint 1.2.2, and this package supports 1.2.0.
const defaultSerde = new MemorySaver().serde;

const loopMetadata: CheckpointMetadata = { source: 'loop', step: 0, parents: {} };

function checkpointWithId(index: number, channelValues: Record<string, unknown> = {}): Checkpoint {
  return { ...emptyCheckpoint(), id: uuid6(index), channel_values: channelValues };
}

function threadConfig(threadId: string): RunnableConfig {
  return { configurable: { thread_id: threadId, checkpoint_ns: '' } };
}

interface WriteCountRow extends Record<string, SqlStorageValue> {
  count: number;
}

function countWrites(storage: DurableObjectStorage): number {
  return storage.sql.exec<WriteCountRow>('SELECT COUNT(*) AS count FROM lc_checkpoint_writes').one().count;
}

describe('persistence', () => {
  it('reads checkpoints back after the Durable Object is evicted', async () => {
    const stub = newObject();
    const checkpoint = checkpointWithId(1, { messages: ['hello'] });
    await inObject(stub, (saver) => saver.put(threadConfig('t'), checkpoint, loopMetadata, {}));

    await evictDurableObject(stub);

    const tuple = await inObject(stub, (saver) => saver.getTuple(threadConfig('t')));
    expect(tuple?.checkpoint).toEqual(checkpoint);
  });

  it('can be constructed more than once on the same storage', async () => {
    await inStorage(newObject(), async (storage) => {
      const first = new DurableObjectSaver(storage);
      const checkpoint = checkpointWithId(1);
      await first.put(threadConfig('t'), checkpoint, loopMetadata, {});
      const second = new DurableObjectSaver(storage);
      expect((await second.getTuple(threadConfig('t')))?.checkpoint.id).toBe(checkpoint.id);
    });
  });
});

describe('upgrading from the schema without task_path', () => {
  it('adds the column and keeps existing rows readable', async () => {
    await inStorage(newObject(), async (storage) => {
      // The schema the in-repo saver used before it became this package.
      storage.sql.exec(`
        CREATE TABLE lc_checkpoints (
          thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '', checkpoint_id TEXT NOT NULL,
          parent_id TEXT, type TEXT NOT NULL, checkpoint BLOB NOT NULL, metadata BLOB NOT NULL,
          PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id))`);
      storage.sql.exec(`
        CREATE TABLE lc_checkpoint_writes (
          thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '', checkpoint_id TEXT NOT NULL,
          task_id TEXT NOT NULL, idx INTEGER NOT NULL, channel TEXT NOT NULL, type TEXT NOT NULL, value BLOB NOT NULL,
          PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx))`);

      const checkpoint = checkpointWithId(1, { messages: ['legacy'] });
      const [type, checkpointBytes] = await defaultSerde.dumpsTyped(checkpoint);
      const [, metadataBytes] = await defaultSerde.dumpsTyped(loopMetadata);
      const [, writeBytes] = await defaultSerde.dumpsTyped('legacy write');
      storage.sql.exec(
        'INSERT INTO lc_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)',
        't',
        '',
        checkpoint.id,
        null,
        type,
        checkpointBytes.slice().buffer,
        metadataBytes.slice().buffer,
      );
      storage.sql.exec(
        'INSERT INTO lc_checkpoint_writes VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        't',
        '',
        checkpoint.id,
        'task',
        0,
        'chan',
        type,
        writeBytes.slice().buffer,
      );

      const saver = new DurableObjectSaver(storage);
      const config = { configurable: { thread_id: 't', checkpoint_ns: '', checkpoint_id: checkpoint.id } };
      await saver.putWrites(config, [['chan', 'new write']], 'task-2', '~path');

      const tuple = await saver.getTuple(config);
      expect(tuple?.checkpoint).toEqual(checkpoint);
      // The legacy row has no path, so it sorts first.
      expect(tuple?.pendingWrites).toEqual([
        ['task', 'chan', 'legacy write'],
        ['task-2', 'chan', 'new write'],
      ]);
    });
  });
});

describe('putWrites', () => {
  it('returns writes in task-path order, not insertion or task-id order', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      await saver.putWrites(config, [['chan', 'second']], 'a-task', '~2');
      await saver.putWrites(config, [['chan', 'first']], 'z-task', '~1');
      const tuple = await saver.getTuple(config);
      expect(tuple?.pendingWrites?.map(([, , value]) => value)).toEqual(['first', 'second']);
    });
  });

  it('keeps the first value of a regular write but replaces special channels', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      await saver.putWrites(config, [['chan', 'original']], 'task');
      await saver.putWrites(config, [['chan', 'retry']], 'task');
      await saver.putWrites(config, [[ERROR, 'first error']], 'task');
      await saver.putWrites(config, [[ERROR, 'latest error']], 'task');
      const tuple = await saver.getTuple(config);
      expect(tuple?.pendingWrites).toEqual([
        ['task', ERROR, 'latest error'],
        ['task', 'chan', 'original'],
      ]);
    });
  });

  it('stores channels named after Object.prototype members as regular writes', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      await saver.putWrites(
        config,
        [
          ['constructor', 1],
          ['toString', 2],
          ['__proto__', 3],
        ],
        'task',
      );
      const tuple = await saver.getTuple(config);
      expect(tuple?.pendingWrites).toEqual([
        ['task', 'constructor', 1],
        ['task', 'toString', 2],
        ['task', '__proto__', 3],
      ]);
    });
  });

  it('writes nothing when any value in the batch is rejected by storage', async () => {
    await inObject(newObject(), async (saver, storage) => {
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      // Larger than the 8 MB Durable Object row limit.
      const oversized = 'x'.repeat(9 * 1024 * 1024);
      await expect(
        saver.putWrites(
          config,
          [
            ['small', 'ok'],
            ['big', oversized],
          ],
          'task',
        ),
      ).rejects.toThrow();
      expect(countWrites(storage)).toBe(0);
    });
  });

  it('writes nothing when serialization fails', async () => {
    const failOnMarker: SerializerProtocol = {
      dumpsTyped: async (data: unknown): Promise<[string, Uint8Array]> => {
        if (data === 'unserializable') throw new Error('boom');
        return defaultSerde.dumpsTyped(data);
      },
      loadsTyped: (type: string, data: Uint8Array | string): Promise<unknown> => defaultSerde.loadsTyped(type, data),
    };
    await inStorage(newObject(), async (storage) => {
      const saver = new DurableObjectSaver(storage, failOnMarker);
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      await expect(
        saver.putWrites(
          config,
          [
            ['small', 'ok'],
            ['bad', 'unserializable'],
          ],
          'task',
        ),
      ).rejects.toThrow('boom');
      expect(countWrites(storage)).toBe(0);
    });
  });
});

describe('put', () => {
  it('rejects a serializer that encodes checkpoint and metadata differently', async () => {
    let calls = 0;
    const serde: SerializerProtocol = {
      dumpsTyped: async (data: unknown): Promise<[string, Uint8Array]> => [
        `type-${calls++}`,
        new TextEncoder().encode(JSON.stringify(data)),
      ],
      loadsTyped: async (_type: string, data: Uint8Array | string): Promise<unknown> =>
        JSON.parse(typeof data === 'string' ? data : new TextDecoder().decode(data)),
    };
    await inStorage(newObject(), async (storage) => {
      const saver = new DurableObjectSaver(storage, serde);
      await expect(saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {})).rejects.toThrow('same type');
    });
  });

  it('records the incoming checkpoint_id as the parent', async () => {
    await inObject(newObject(), async (saver) => {
      const parent = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      const child = await saver.put(parent, checkpointWithId(2), loopMetadata, {});
      expect((await saver.getTuple(child))?.parentConfig).toEqual(parent);
    });
  });

  it('accepts numeric thread ids and returns them as strings', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put({ configurable: { thread_id: 42 } }, checkpointWithId(1), loopMetadata, {});
      expect(config.configurable?.thread_id).toBe('42');
      expect(await saver.getTuple({ configurable: { thread_id: 42 } })).toBeDefined();
    });
  });

  it('rejects ids that are not strings or numbers', async () => {
    await inObject(newObject(), async (saver) => {
      await expect(saver.put({ configurable: { thread_id: { id: 1 } } }, checkpointWithId(1), loopMetadata, {})).rejects.toThrow(TypeError);
      await expect(saver.getTuple({ configurable: { thread_id: 't', checkpoint_id: ['x'] } })).rejects.toThrow(TypeError);
    });
  });
});

describe('list', () => {
  it('applies the limit after the metadata filter', async () => {
    await inObject(newObject(), async (saver) => {
      let config = threadConfig('t');
      const ids: string[] = [];
      for (let i = 1; i <= 5; i++) {
        const metadata: CheckpointMetadata = { source: i % 2 === 0 ? 'input' : 'loop', step: i, parents: {} };
        const checkpoint = checkpointWithId(i);
        ids.push(checkpoint.id);
        config = await saver.put(config, checkpoint, metadata, {});
      }
      const tuples = await collect(saver.list(threadConfig('t'), { filter: { source: 'loop' }, limit: 2 }));
      // Newest first: checkpoints 5 and 3 are the latest "loop" ones.
      expect(tuples.map((t) => t.checkpoint.id)).toEqual([ids[4], ids[2]]);
    });
  });

  it('matches nested filter values structurally', async () => {
    await inObject(newObject(), async (saver) => {
      const metadata = { source: 'loop', step: 0, parents: {}, tags: { b: 2, a: [1, { c: 3 }] } } as CheckpointMetadata;
      await saver.put(threadConfig('t'), checkpointWithId(1), metadata, {});
      const match = await collect(saver.list(threadConfig('t'), { filter: { tags: { a: [1, { c: 3 }], b: 2 } } }));
      const miss = await collect(saver.list(threadConfig('t'), { filter: { tags: { a: [1, { c: 4 }], b: 2 } } }));
      expect(match).toHaveLength(1);
      expect(miss).toHaveLength(0);
    });
  });

  it('rejects limits that are not non-negative integers', async () => {
    await inObject(newObject(), async (saver) => {
      for (const limit of [-1, 1.5, Number.NaN]) {
        await expect(collect(saver.list(threadConfig('t'), { limit }))).rejects.toThrow(RangeError);
      }
    });
  });

  it('skips checkpoints deleted while the caller is iterating', async () => {
    await inObject(newObject(), async (saver) => {
      let config = threadConfig('t');
      config = await saver.put(config, checkpointWithId(1), loopMetadata, {});
      const newest = checkpointWithId(2);
      await saver.put(config, newest, loopMetadata, {});
      const seen: string[] = [];
      for await (const tuple of saver.list(threadConfig('t'))) {
        seen.push(tuple.checkpoint.id);
        await saver.deleteThread('t');
      }
      expect(seen).toEqual([newest.id]);
    });
  });
});

describe('empty checkpoint ids', () => {
  // LangGraph's getCheckpointId() returns "" for "no checkpoint".
  it('treats checkpoint_id "" as "latest"', async () => {
    await inObject(newObject(), async (saver) => {
      const checkpoint = checkpointWithId(1);
      await saver.put(threadConfig('t'), checkpoint, loopMetadata, {});
      const tuple = await saver.getTuple({ configurable: { thread_id: 't', checkpoint_id: '' } });
      expect(tuple?.checkpoint.id).toBe(checkpoint.id);
    });
  });

  it('does not record "" as a parent', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put({ configurable: { thread_id: 't', checkpoint_id: '' } }, checkpointWithId(1), loopMetadata, {});
      expect((await saver.getTuple(config))?.parentConfig).toBeUndefined();
    });
  });

  it('ignores before: { checkpoint_id: "" }', async () => {
    await inObject(newObject(), async (saver) => {
      await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      const listed = await collect(saver.list(threadConfig('t'), { before: { configurable: { checkpoint_id: '' } } }));
      expect(listed).toHaveLength(1);
    });
  });
});

describe('list paging', () => {
  it('returns every checkpoint once across pages, including ids shared by several threads', async () => {
    await inObject(newObject(), async (saver) => {
      const ids: string[] = [];
      for (let i = 0; i < 230; i++) ids.push(uuid6(i));
      // Same checkpoint ids in two threads, so pages break on ties.
      for (const threadId of ['a', 'b']) {
        for (const id of ids) {
          await saver.put({ configurable: { thread_id: threadId } }, { ...emptyCheckpoint(), id }, loopMetadata, {});
        }
      }
      const all = await collect(saver.list({ configurable: {} }));
      const keys = all.map((t) => `${t.config.configurable?.thread_id}/${t.checkpoint.id}`);
      expect(new Set(keys).size).toBe(460);
      const sortedIds = all.map((t) => t.checkpoint.id);
      expect(sortedIds).toEqual([...sortedIds].sort().reverse());

      const filtered = await collect(saver.list({ configurable: { thread_id: 'b' } }, { filter: { source: 'loop' }, limit: 150 }));
      expect(filtered).toHaveLength(150);
      expect(filtered.every((t) => t.config.configurable?.thread_id === 'b')).toBe(true);
    });
  });
});

describe('corrupt rows', () => {
  it('throws instead of allocating when a BLOB column holds a number', async () => {
    await inObject(newObject(), async (saver, storage) => {
      const config = await saver.put(threadConfig('t'), checkpointWithId(1), loopMetadata, {});
      storage.sql.exec('UPDATE lc_checkpoints SET checkpoint = 100000000');
      await expect(saver.getTuple(config)).rejects.toThrow('unexpected type number');
    });
  });
});

describe('id validation', () => {
  it('rejects numbers that are not safe integers', async () => {
    await inObject(newObject(), async (saver) => {
      for (const threadId of [1.5, Number.NaN, 2 ** 60]) {
        await expect(saver.put({ configurable: { thread_id: threadId } }, checkpointWithId(1), loopMetadata, {})).rejects.toThrow(
          TypeError,
        );
      }
    });
  });

  it('rejects a non-string checkpoint.id', async () => {
    await inObject(newObject(), async (saver) => {
      const checkpoint = { ...emptyCheckpoint(), id: 10 } as unknown as Checkpoint;
      await expect(saver.put(threadConfig('t'), checkpoint, loopMetadata, {})).rejects.toThrow(TypeError);
    });
  });
});
