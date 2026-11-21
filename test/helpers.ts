import {
  BaseCheckpointSaver,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointTuple,
  type PendingWrite,
} from '@langchain/langgraph-checkpoint';
import type { RunnableConfig } from '@langchain/core/runnables';
import { env, runInDurableObject } from 'cloudflare:test';
import type { DurableObjectSaver } from '../src/index';
import type { TestObject } from './worker';

export type TestObjectStub = DurableObjectStub<TestObject>;

export type ObjectCallback<R> = (saver: DurableObjectSaver, storage: DurableObjectStorage) => R | Promise<R>;

export type StorageCallback<R> = (storage: DurableObjectStorage) => R | Promise<R>;

export function newObject(): TestObjectStub {
  return env.TEST_OBJECT.get(env.TEST_OBJECT.newUniqueId());
}

/** Run `callback` inside the Durable Object, where its storage may be used. */
export function inObject<R>(stub: TestObjectStub, callback: ObjectCallback<R>): Promise<R> {
  return runInDurableObject(stub, (instance: TestObject, state: DurableObjectState) => callback(instance.saver, state.storage));
}

/** Like `inObject`, but without creating the object's saver (and so its tables) first. */
export function inStorage<R>(stub: TestObjectStub, callback: StorageCallback<R>): Promise<R> {
  return runInDurableObject(stub, (_instance: TestObject, state: DurableObjectState) => callback(state.storage));
}

export async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const item of items) out.push(item);
  return out;
}

/**
 * Forwards every call into a Durable Object. Storage handles can't be used
 * outside their object's request context, but the validation suite calls the
 * saver directly from test code.
 */
export class ForwardingSaver extends BaseCheckpointSaver {
  constructor(private readonly stub: TestObjectStub) {
    super();
  }

  getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    return inObject(this.stub, (saver) => saver.getTuple(config));
  }

  async *list(config: RunnableConfig, options?: CheckpointListOptions): AsyncGenerator<CheckpointTuple> {
    yield* await inObject(this.stub, (saver) => collect(saver.list(config, options)));
  }

  put(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, newVersions: ChannelVersions): Promise<RunnableConfig> {
    return inObject(this.stub, (saver) => saver.put(config, checkpoint, metadata, newVersions));
  }

  putWrites(config: RunnableConfig, writes: PendingWrite[], taskId: string, taskPath?: string): Promise<void> {
    return inObject(this.stub, (saver) => saver.putWrites(config, writes, taskId, taskPath));
  }

  deleteThread(threadId: string): Promise<void> {
    return inObject(this.stub, (saver) => saver.deleteThread(threadId));
  }
}
