import {
  BaseCheckpointSaver,
  TASKS,
  WRITES_IDX_MAP,
  compareWritesSortKeys,
  copyCheckpoint,
  maxChannelVersion,
  writesSortKey,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointPendingWrite,
  type CheckpointTuple,
  type PendingWrite,
  type SerializerProtocol,
} from '@langchain/langgraph-checkpoint';
import type { RunnableConfig } from '@langchain/core/runnables';

/** A value the Durable Object SQL API can bind or return. */
export type SqlValue = ArrayBuffer | string | number | null;

/** One result row from the Durable Object SQL API. */
export type SqlRow = Record<string, SqlValue>;

/** The part of `SqlStorageCursor` this saver uses. */
export interface SqlCursor<T extends SqlRow> {
  toArray(): T[];
}

/** The part of `SqlStorage` (`ctx.storage.sql`) this saver uses. */
export interface SqlExecutor {
  exec<T extends SqlRow>(query: string, ...bindings: SqlValue[]): SqlCursor<T>;
}

/**
 * The part of `DurableObjectStorage` (`ctx.storage`) this saver uses.
 *
 * Declared structurally so the published types don't depend on
 * `@cloudflare/workers-types` being installed in a particular way.
 */
export interface DurableObjectStorageLike {
  sql: SqlExecutor;
  transactionSync<T>(closure: () => T): T;
}

interface CheckpointRow extends SqlRow {
  thread_id: string;
  checkpoint_ns: string;
  checkpoint_id: string;
  parent_id: string | null;
  type: string | null;
  checkpoint: ArrayBuffer | string;
  metadata: ArrayBuffer | string;
}

interface CandidateRow extends SqlRow {
  thread_id: string;
  checkpoint_ns: string;
  checkpoint_id: string;
  // Only selected when list() has a metadata filter.
  type: string | null;
  metadata: ArrayBuffer | string;
}

interface WriteRow extends SqlRow {
  task_id: string;
  task_path: string;
  idx: number;
  channel: string;
  type: string | null;
  value: ArrayBuffer | string;
}

interface SendRow extends SqlRow {
  task_id: string;
  task_path: string;
  idx: number;
  type: string | null;
  value: ArrayBuffer | string;
}

interface ColumnRow extends SqlRow {
  name: string;
}

interface CheckpointKey {
  threadId: string;
  checkpointNs: string;
  checkpointId: string;
}

type FilterEntry = [string, unknown];

// Table names match the saver this package was extracted from, so existing
// Durable Objects keep their history after upgrading.
const CHECKPOINTS_TABLE = 'lc_checkpoints';
const WRITES_TABLE = 'lc_checkpoint_writes';

const LIST_PAGE_SIZE = 100;

/**
 * LangGraph checkpoint saver backed by a Durable Object's SQLite storage.
 *
 * ```ts
 * export class Chat extends DurableObject {
 *   checkpointer = new DurableObjectSaver(this.ctx.storage);
 * }
 * ```
 *
 * Every Durable Object has its own database, so each object only ever sees
 * its own checkpoints. `thread_id` partitions data inside one object; it is
 * not an access-control boundary.
 */
export class DurableObjectSaver extends BaseCheckpointSaver {
  private readonly storage: DurableObjectStorageLike;

  constructor(storage: DurableObjectStorageLike, serde?: SerializerProtocol) {
    super(serde);
    this.storage = storage;
    this.storage.transactionSync(() => this.createTables());
  }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const threadId = readId(config.configurable?.thread_id, 'thread_id');
    if (threadId === undefined) return undefined;
    const checkpointNs = readId(config.configurable?.checkpoint_ns, 'checkpoint_ns') ?? '';
    const checkpointId = readCheckpointId(config.configurable?.checkpoint_id, 'checkpoint_id');

    const row =
      checkpointId === undefined
        ? this.storage.sql
            .exec<CheckpointRow>(
              `SELECT * FROM ${CHECKPOINTS_TABLE} WHERE thread_id = ? AND checkpoint_ns = ? ORDER BY checkpoint_id DESC LIMIT 1`,
              threadId,
              checkpointNs,
            )
            .toArray()[0]
        : this.readCheckpointRow({ threadId, checkpointNs, checkpointId });

    return row === undefined ? undefined : this.toTuple(row);
  }

  async *list(config: RunnableConfig, options?: CheckpointListOptions): AsyncGenerator<CheckpointTuple> {
    const threadId = readId(config.configurable?.thread_id, 'thread_id');
    const checkpointNs = readId(config.configurable?.checkpoint_ns, 'checkpoint_ns');
    const before = readCheckpointId(options?.before?.configurable?.checkpoint_id, 'before.checkpoint_id');
    const limit = readLimit(options?.limit);
    const filter: FilterEntry[] = Object.entries(options?.filter ?? {}).filter(([, value]) => value !== undefined);

    const where: string[] = [];
    const bindings: SqlValue[] = [];
    if (threadId !== undefined) {
      where.push('thread_id = ?');
      bindings.push(threadId);
    }
    if (checkpointNs !== undefined) {
      where.push('checkpoint_ns = ?');
      bindings.push(checkpointNs);
    }
    if (before !== undefined) {
      where.push('checkpoint_id < ?');
      bindings.push(before);
    }

    // Page through keys rather than loading the whole history: a long thread
    // can hold far more metadata than fits comfortably in memory. Ids can
    // repeat across threads and namespaces, so the page key is all three.
    const columns =
      filter.length > 0 ? 'thread_id, checkpoint_ns, checkpoint_id, type, metadata' : 'thread_id, checkpoint_ns, checkpoint_id';
    let after: CandidateRow | undefined;
    let yielded = 0;
    while (limit === undefined || yielded < limit) {
      const pageWhere = [...where];
      const pageBindings = [...bindings];
      if (after !== undefined) {
        pageWhere.push('(checkpoint_id, thread_id, checkpoint_ns) < (?, ?, ?)');
        pageBindings.push(after.checkpoint_id, after.thread_id, after.checkpoint_ns);
      }
      let query = `SELECT ${columns} FROM ${CHECKPOINTS_TABLE}`;
      if (pageWhere.length > 0) query += ` WHERE ${pageWhere.join(' AND ')}`;
      query += ' ORDER BY checkpoint_id DESC, thread_id DESC, checkpoint_ns DESC LIMIT ?';
      pageBindings.push(LIST_PAGE_SIZE);

      // Read each page in one go: a cursor held across `await` doesn't see a stable snapshot.
      const page = this.storage.sql.exec<CandidateRow>(query, ...pageBindings).toArray();
      for (const candidate of page) {
        if (limit !== undefined && yielded >= limit) return;
        if (filter.length > 0) {
          const metadata: unknown = await this.serde.loadsTyped(candidate.type ?? 'json', fromBlob(candidate.metadata));
          if (!matchesFilter(metadata, filter)) continue;
        }
        const row = this.readCheckpointRow({
          threadId: candidate.thread_id,
          checkpointNs: candidate.checkpoint_ns,
          checkpointId: candidate.checkpoint_id,
        });
        // Deleted while the caller was consuming earlier results.
        if (row === undefined) continue;
        yield await this.toTuple(row);
        yielded++;
      }
      if (page.length < LIST_PAGE_SIZE) return;
      after = page[page.length - 1];
    }
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    _newVersions: ChannelVersions,
  ): Promise<RunnableConfig> {
    if (config.configurable === undefined) throw new Error('Empty configuration supplied.');
    const threadId = readId(config.configurable.thread_id, 'thread_id');
    if (threadId === undefined) throw new Error('Missing "thread_id" field in passed "config.configurable".');
    const checkpointNs = readId(config.configurable.checkpoint_ns, 'checkpoint_ns') ?? '';
    const parentId = readCheckpointId(config.configurable.checkpoint_id, 'checkpoint_id') ?? null;
    // Checkpoint ids are uuid6 strings; their text order is their time order.
    if (typeof checkpoint.id !== 'string') throw new TypeError('checkpoint.id must be a string.');
    const checkpointId = readCheckpointId(checkpoint.id, 'checkpoint.id');
    if (checkpointId === undefined) throw new Error('Checkpoint is missing an id.');

    const [[type, serializedCheckpoint], [metadataType, serializedMetadata]] = await Promise.all([
      this.serde.dumpsTyped(copyCheckpoint(checkpoint)),
      this.serde.dumpsTyped(metadata),
    ]);
    // One `type` column describes both blobs.
    if (type !== metadataType) throw new Error('Failed to serialize checkpoint and metadata to the same type.');

    this.storage.sql.exec(
      `INSERT OR REPLACE INTO ${CHECKPOINTS_TABLE}
       (thread_id, checkpoint_ns, checkpoint_id, parent_id, type, checkpoint, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      threadId,
      checkpointNs,
      checkpointId,
      parentId,
      type,
      toBlob(serializedCheckpoint),
      toBlob(serializedMetadata),
    );

    return { configurable: { thread_id: threadId, checkpoint_ns: checkpointNs, checkpoint_id: checkpointId } };
  }

  async putWrites(config: RunnableConfig, writes: PendingWrite[], taskId: string, taskPath?: string): Promise<void> {
    if (config.configurable === undefined) throw new Error('Empty configuration supplied.');
    const threadId = readId(config.configurable.thread_id, 'thread_id');
    if (threadId === undefined) throw new Error('Missing thread_id field in config.configurable.');
    const checkpointId = readCheckpointId(config.configurable.checkpoint_id, 'checkpoint_id');
    if (checkpointId === undefined) throw new Error('Missing checkpoint_id field in config.configurable.');
    const checkpointNs = readId(config.configurable.checkpoint_ns, 'checkpoint_ns') ?? '';
    const task = readId(taskId, 'taskId');
    if (task === undefined) throw new Error('Missing taskId.');
    const path = readId(taskPath, 'taskPath') ?? '';

    // Special channels (errors, interrupts...) overwrite; regular writes are
    // write-once, so a retried task can't clobber what was already recorded.
    const conflict = writes.every(([channel]) => specialWriteIndex(channel) !== undefined) ? 'REPLACE' : 'IGNORE';

    // Serialize first: a failure here must not leave a partial batch behind.
    const rows = await Promise.all(
      writes.map(async ([channel, value], idx) => {
        if (readId(channel, 'channel') === undefined) throw new TypeError('channel must be a string.');
        const [type, serialized] = await this.serde.dumpsTyped(value);
        return [specialWriteIndex(channel) ?? idx, channel, type, toBlob(serialized)] as const;
      }),
    );

    this.storage.transactionSync(() => {
      for (const [idx, channel, type, value] of rows) {
        this.storage.sql.exec(
          `INSERT OR ${conflict} INTO ${WRITES_TABLE}
           (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, value)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          threadId,
          checkpointNs,
          checkpointId,
          task,
          path,
          idx,
          channel,
          type,
          value,
        );
      }
    });
  }

  async deleteThread(threadId: string): Promise<void> {
    const id = readId(threadId, 'threadId');
    if (id === undefined) throw new Error('Missing threadId.');
    this.storage.transactionSync(() => {
      this.storage.sql.exec(`DELETE FROM ${CHECKPOINTS_TABLE} WHERE thread_id = ?`, id);
      this.storage.sql.exec(`DELETE FROM ${WRITES_TABLE} WHERE thread_id = ?`, id);
    });
  }

  private createTables(): void {
    this.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS ${CHECKPOINTS_TABLE} (
        thread_id      TEXT    NOT NULL,
        checkpoint_ns  TEXT    NOT NULL DEFAULT '',
        checkpoint_id  TEXT    NOT NULL,
        parent_id      TEXT,
        type           TEXT    NOT NULL,
        checkpoint     BLOB    NOT NULL,
        metadata       BLOB    NOT NULL,
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
      )
    `);
    this.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS ${WRITES_TABLE} (
        thread_id      TEXT    NOT NULL,
        checkpoint_ns  TEXT    NOT NULL DEFAULT '',
        checkpoint_id  TEXT    NOT NULL,
        task_id        TEXT    NOT NULL,
        task_path      TEXT    NOT NULL DEFAULT '',
        idx            INTEGER NOT NULL,
        channel        TEXT    NOT NULL,
        type           TEXT    NOT NULL,
        value          BLOB    NOT NULL,
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
      )
    `);
    // Tables created before task_path was stored lack the column.
    const hasTaskPath =
      this.storage.sql.exec<ColumnRow>(`SELECT name FROM pragma_table_info('${WRITES_TABLE}') WHERE name = 'task_path'`).toArray().length >
      0;
    if (!hasTaskPath) {
      this.storage.sql.exec(`ALTER TABLE ${WRITES_TABLE} ADD COLUMN task_path TEXT NOT NULL DEFAULT ''`);
    }
  }

  private readCheckpointRow(key: CheckpointKey): CheckpointRow | undefined {
    return this.storage.sql
      .exec<CheckpointRow>(
        `SELECT * FROM ${CHECKPOINTS_TABLE} WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?`,
        key.threadId,
        key.checkpointNs,
        key.checkpointId,
      )
      .toArray()[0];
  }

  private async toTuple(row: CheckpointRow): Promise<CheckpointTuple> {
    // Read the writes before the first await so they match the checkpoint row.
    const writeRows = sortWrites(
      this.storage.sql
        .exec<WriteRow>(
          `SELECT task_id, task_path, idx, channel, type, value FROM ${WRITES_TABLE}
           WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?`,
          row.thread_id,
          row.checkpoint_ns,
          row.checkpoint_id,
        )
        .toArray(),
    );
    const type = row.type ?? 'json';
    const checkpoint: Checkpoint = await this.serde.loadsTyped(type, fromBlob(row.checkpoint));
    const metadata: CheckpointMetadata = await this.serde.loadsTyped(type, fromBlob(row.metadata));
    const pendingWrites: CheckpointPendingWrite[] = await Promise.all(
      writeRows.map(async (w): Promise<CheckpointPendingWrite> => [
        w.task_id,
        w.channel,
        await this.serde.loadsTyped(w.type ?? 'json', fromBlob(w.value)),
      ]),
    );

    // Before checkpoint format v4, `Send`s lived as writes on the parent
    // checkpoint rather than in the checkpoint itself.
    // Only legacy checkpoints need these, so they're read here rather than up front.
    if (checkpoint.v < 4 && row.parent_id !== null) {
      const sendRows = sortWrites(
        this.storage.sql
          .exec<SendRow>(
            `SELECT task_id, task_path, idx, type, value FROM ${WRITES_TABLE}
             WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ? AND channel = ?`,
            row.thread_id,
            row.checkpoint_ns,
            row.parent_id,
            TASKS,
          )
          .toArray(),
      );
      checkpoint.channel_values ??= {};
      checkpoint.channel_versions ??= {};
      checkpoint.channel_values[TASKS] = await Promise.all(sendRows.map((s) => this.serde.loadsTyped(s.type ?? 'json', fromBlob(s.value))));
      const versions = Object.values(checkpoint.channel_versions);
      checkpoint.channel_versions[TASKS] = versions.length > 0 ? maxChannelVersion(...versions) : this.getNextVersion(undefined);
    }

    const parentConfig: RunnableConfig | undefined =
      row.parent_id === null
        ? undefined
        : { configurable: { thread_id: row.thread_id, checkpoint_ns: row.checkpoint_ns, checkpoint_id: row.parent_id } };

    return {
      config: { configurable: { thread_id: row.thread_id, checkpoint_ns: row.checkpoint_ns, checkpoint_id: row.checkpoint_id } },
      checkpoint,
      metadata,
      parentConfig,
      pendingWrites,
    };
  }
}

/**
 * Normalise an id from a config. Integers are accepted because callers often
 * pass numeric thread ids; anything else is a caller bug and is rejected
 * rather than stored as "[object Object]".
 *
 * Strings with lone UTF-16 surrogates (e.g. user text cut mid-emoji) are
 * rejected: SQLite stores them as U+FFFD, so they would come back as a
 * different id and the thread's history would silently fork.
 */
function readId(value: unknown, name: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === 'string') {
    if (!value.isWellFormed()) throw new TypeError(`${name} contains unpaired surrogate characters.`);
    return value;
  }
  if (typeof value === 'number' && Number.isSafeInteger(value)) return String(value);
  throw new TypeError(`${name} must be a string or integer, got ${typeof value}.`);
}

/** LangGraph uses "" for "no checkpoint" (see `getCheckpointId`), so it means "latest", not an id. */
function readCheckpointId(value: unknown, name: string): string | undefined {
  return readId(value, name) || undefined;
}

function readLimit(limit: unknown): number | undefined {
  if (limit === undefined || limit === null) return undefined;
  if (typeof limit !== 'number' || !Number.isSafeInteger(limit) || limit < 0) {
    throw new RangeError('limit must be a non-negative integer.');
  }
  return limit;
}

// `hasOwn`, not `in`: a channel named "constructor" or "toString" must not
// pick up Object.prototype members as its index.
function specialWriteIndex(channel: string): number | undefined {
  return Object.hasOwn(WRITES_IDX_MAP, channel) ? WRITES_IDX_MAP[channel] : undefined;
}

/** Live execution applies a superstep's writes in `writesSortKey` order; replay must match it. */
function sortWrites<T extends SendRow>(rows: T[]): T[] {
  return rows
    .map((row) => ({ row, key: writesSortKey(row.task_path, row.task_id, row.idx) }))
    .sort((a, b) => compareWritesSortKeys(a.key, b.key))
    .map(({ row }) => row);
}

function matchesFilter(metadata: unknown, filter: FilterEntry[]): boolean {
  if (!isRecord(metadata)) return false;
  return filter.every(([key, value]) => Object.hasOwn(metadata, key) && deepEqual(metadata[key], value));
}

/** Plain objects only: a revived Map, Set or RegExp has no own keys and would equal `{}`. */
function isRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Structural equality for JSON-like metadata values. */
function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((item, i) => deepEqual(item, b[i]));
  }
  if (isRecord(a) && isRecord(b)) {
    const keys = Object.keys(a);
    return keys.length === Object.keys(b).length && keys.every((key) => Object.hasOwn(b, key) && deepEqual(a[key], b[key]));
  }
  return false;
}

/** Bind only the bytes the serializer produced, not the whole backing buffer. */
function toBlob(data: Uint8Array): ArrayBuffer {
  return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
}

/** SQLite returns BLOBs as ArrayBuffer; the serializer wants bytes or text. */
function fromBlob(data: SqlValue): Uint8Array | string {
  if (typeof data === 'string') return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  // A corrupt row; `new Uint8Array(number)` would allocate rather than fail.
  throw new TypeError(`Stored value has unexpected type ${data === null ? 'null' : typeof data}.`);
}
