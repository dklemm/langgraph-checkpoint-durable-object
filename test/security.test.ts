import { emptyCheckpoint, uuid6, type Checkpoint, type CheckpointMetadata } from '@langchain/langgraph-checkpoint';
import { describe, expect, it } from 'vitest';
import { collect, inObject, newObject } from './helpers';

const metadata: CheckpointMetadata = { source: 'loop', step: 0, parents: {} };

function checkpointWithId(index: number, channelValues: Record<string, unknown> = {}): Checkpoint {
  return { ...emptyCheckpoint(), id: uuid6(index), channel_values: channelValues };
}

interface TableRow extends Record<string, SqlStorageValue> {
  name: string;
}

const injectionPayloads = [
  "' OR '1'='1",
  "'; DROP TABLE lc_checkpoints; --",
  '" OR ""="',
  '1) OR (1=1',
  "x' UNION SELECT * FROM lc_checkpoint_writes --",
  '$.source',
  '\u0000nul',
  '%',
  '_',
];

describe('SQL injection', () => {
  it.each(injectionPayloads)('stores and finds ids containing %j literally', async (payload) => {
    await inObject(newObject(), async (saver, storage) => {
      // A bystander thread that a successful injection would expose or delete.
      await saver.put({ configurable: { thread_id: 'bystander' } }, checkpointWithId(1), metadata, {});

      const checkpoint = checkpointWithId(2);
      const config = await saver.put({ configurable: { thread_id: payload, checkpoint_ns: payload } }, checkpoint, metadata, {});
      await saver.putWrites(config, [[payload, payload]], payload, payload);

      const tuple = await saver.getTuple(config);
      expect(tuple?.config.configurable?.thread_id).toBe(payload);
      expect(tuple?.pendingWrites).toEqual([[payload, payload, payload]]);

      const listed = await collect(saver.list({ configurable: { thread_id: payload } }));
      expect(listed.map((t) => t.checkpoint.id)).toEqual([checkpoint.id]);

      expect(await saver.getTuple({ configurable: { thread_id: 'bystander', checkpoint_id: payload } })).toBeUndefined();

      await saver.deleteThread(payload);
      expect(await saver.getTuple({ configurable: { thread_id: 'bystander' } })).toBeDefined();
      const tables = storage.sql.exec<TableRow>("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'lc_%'").toArray();
      expect(tables.map((t) => t.name).sort()).toEqual(['lc_checkpoint_writes', 'lc_checkpoints']);
    });
  });

  it.each(injectionPayloads)('treats filter key %j as data', async (payload) => {
    await inObject(newObject(), async (saver) => {
      await saver.put({ configurable: { thread_id: 't' } }, checkpointWithId(1), metadata, {});
      expect(await collect(saver.list({ configurable: {} }, { filter: { [payload]: 'loop' } }))).toEqual([]);
      expect(await collect(saver.list({ configurable: {} }, { filter: { source: payload } }))).toEqual([]);
    });
  });
});

describe('thread isolation', () => {
  it('lists only the requested thread', async () => {
    await inObject(newObject(), async (saver) => {
      await saver.put({ configurable: { thread_id: 'a' } }, checkpointWithId(1), metadata, {});
      await saver.put({ configurable: { thread_id: 'b' } }, checkpointWithId(2), metadata, {});
      const listed = await collect(saver.list({ configurable: { thread_id: 'a' } }));
      expect(listed.map((t) => t.config.configurable?.thread_id)).toEqual(['a']);
    });
  });

  it('does not treat an empty thread_id as "all threads"', async () => {
    await inObject(newObject(), async (saver) => {
      await saver.put({ configurable: { thread_id: 'a' } }, checkpointWithId(1), metadata, {});
      expect(await collect(saver.list({ configurable: { thread_id: '' } }))).toEqual([]);
    });
  });

  it('does not return another thread’s checkpoint when given its checkpoint_id', async () => {
    await inObject(newObject(), async (saver) => {
      const a = await saver.put({ configurable: { thread_id: 'a' } }, checkpointWithId(1), metadata, {});
      const stolen = { configurable: { thread_id: 'b', checkpoint_id: a.configurable?.checkpoint_id } };
      expect(await saver.getTuple(stolen)).toBeUndefined();
    });
  });

  it('keeps separate Durable Objects fully separate', async () => {
    const first = newObject();
    const second = newObject();
    await inObject(first, (saver) => saver.put({ configurable: { thread_id: 'shared-name' } }, checkpointWithId(1), metadata, {}));
    expect(await inObject(second, (saver) => saver.getTuple({ configurable: { thread_id: 'shared-name' } }))).toBeUndefined();
  });
});

describe('unpaired surrogates', () => {
  // SQLite would store these as U+FFFD and hand back a different id.
  const truncatedEmoji = 'chat-😀'.slice(0, 6);

  it('rejects them in ids', async () => {
    await inObject(newObject(), async (saver) => {
      await expect(saver.put({ configurable: { thread_id: truncatedEmoji } }, checkpointWithId(1), metadata, {})).rejects.toThrow(
        'surrogate',
      );
      await expect(saver.getTuple({ configurable: { thread_id: truncatedEmoji } })).rejects.toThrow('surrogate');
      await expect(saver.deleteThread(truncatedEmoji)).rejects.toThrow('surrogate');
    });
  });

  it('rejects them in channel, task id and task path', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put({ configurable: { thread_id: 't' } }, checkpointWithId(1), metadata, {});
      await expect(saver.putWrites(config, [[truncatedEmoji, 1]], 'task')).rejects.toThrow('surrogate');
      await expect(saver.putWrites(config, [['chan', 1]], truncatedEmoji)).rejects.toThrow('surrogate');
      await expect(saver.putWrites(config, [['chan', 1]], 'task', truncatedEmoji)).rejects.toThrow('surrogate');
    });
  });

  it('accepts well-formed emoji', async () => {
    await inObject(newObject(), async (saver) => {
      const config = await saver.put({ configurable: { thread_id: 'chat-😀' } }, checkpointWithId(1), metadata, {});
      expect((await saver.getTuple(config))?.config.configurable?.thread_id).toBe('chat-😀');
    });
  });
});

describe('metadata filter', () => {
  it('does not treat revived non-plain objects as equal', async () => {
    await inObject(newObject(), async (saver) => {
      const tagged = { ...metadata, tags: new Map([['owner', 'alice']]) } as CheckpointMetadata;
      await saver.put({ configurable: { thread_id: 't' } }, checkpointWithId(1), tagged, {});
      expect(await collect(saver.list({ configurable: {} }, { filter: { tags: new Map([['owner', 'mallory']]) } }))).toEqual([]);
      expect(await collect(saver.list({ configurable: {} }, { filter: { tags: {} } }))).toEqual([]);
    });
  });

  it.each(['__proto__', 'constructor', 'toString', 'hasOwnProperty'])('does not match inherited property %j', async (key) => {
    await inObject(newObject(), async (saver) => {
      await saver.put({ configurable: { thread_id: 't' } }, checkpointWithId(1), metadata, {});
      const filter = JSON.parse(`{"${key}": {}}`) as Record<string, unknown>;
      expect(await collect(saver.list({ configurable: {} }, { filter }))).toEqual([]);
    });
  });
});

// These pin the behaviour of LangChain's JsonPlusSerializer, which every
// LangGraph saver uses. Plain objects in state are not escaped, so data shaped
// like a serialized LangChain object is revived on read. If one of these
// starts failing after an upgrade, upstream behaviour changed: update the
// "Security" section of the README.
describe('deserialization (upstream serializer behaviour)', () => {
  async function roundTrip(value: unknown): Promise<unknown> {
    return inObject(newObject(), async (saver) => {
      const config = await saver.put({ configurable: { thread_id: 't' } }, checkpointWithId(1, { user: value }), metadata, {});
      return (await saver.getTuple(config))?.checkpoint.channel_values.user;
    });
  }

  it('never resolves secret references from the environment', async () => {
    const secretLookalike = { lc: 1, type: 'secret', id: ['OPENAI_API_KEY'] };
    expect(await roundTrip(secretLookalike)).toEqual(secretLookalike);
    const secretInsideConstructor = {
      lc: 1,
      type: 'constructor',
      id: ['langchain_core', 'messages', 'HumanMessage'],
      kwargs: { content: secretLookalike },
    };
    await expect(roundTrip(secretInsideConstructor)).rejects.toThrow('Missing secret');
  });

  it('revives constructor-shaped data into langchain_core classes', async () => {
    const lookalike = { lc: 1, type: 'constructor', id: ['langchain_core', 'messages', 'HumanMessage'], kwargs: { content: 'hi' } };
    const revived = await roundTrip(lookalike);
    expect(revived?.constructor.name).toBe('HumanMessage');
  });

  it('fails to read a checkpoint holding constructor-shaped data for other classes', async () => {
    const lookalike = { lc: 1, type: 'constructor', id: ['langchain', 'chat_models', 'openai', 'ChatOpenAI'], kwargs: {} };
    await expect(roundTrip(lookalike)).rejects.toThrow('Invalid namespace');
  });
});
