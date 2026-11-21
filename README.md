# @dklemm/langgraph-checkpoint-durable-object

A [LangGraph.js](https://github.com/langchain-ai/langgraphjs) checkpoint saver for Cloudflare Durable Objects. It stores checkpoints in the
object's built-in SQLite database, so graph state survives hibernation, eviction and redeploys without an external database.

The official `@langchain/langgraph-checkpoint-sqlite` saver needs `better-sqlite3`, a Node.js native addon that can't run in Workers. This
package keeps the same data model on top of the Durable Object SQL API.

## Install

```sh
npm install @dklemm/langgraph-checkpoint-durable-object
```

It needs these peer dependencies: `@langchain/langgraph-checkpoint` `^1.2.0` and `@langchain/core` `^1.1.48`.

## Use

Pass the object's storage to the saver, then compile your graph with it:

```ts
import { DurableObject } from 'cloudflare:workers';
import { DurableObjectSaver } from '@dklemm/langgraph-checkpoint-durable-object';
import { buildGraph } from './graph';

export class ChatSession extends DurableObject<Env> {
  private graph = buildGraph().compile({ checkpointer: new DurableObjectSaver(this.ctx.storage) });

  async reply(text: string): Promise<string> {
    const result = await this.graph.invoke({ messages: [{ role: 'user', content: text }] }, { configurable: { thread_id: 'main' } });
    return result.messages.at(-1).content;
  }
}
```

The Durable Object class must use SQLite storage:

```jsonc
// wrangler.jsonc
{
  "compatibility_flags": ["nodejs_compat"],
  "durable_objects": { "bindings": [{ "name": "CHAT_SESSION", "class_name": "ChatSession" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["ChatSession"] }],
}
```

### Using `interrupt()`

LangGraph's `interrupt()` needs `AsyncLocalStorage`. On Workers, enable `nodejs_compat` and register it once at startup:

```ts
import { AsyncLocalStorage } from 'node:async_hooks';
import { AsyncLocalStorageProviderSingleton } from '@langchain/core/singletons';

AsyncLocalStorageProviderSingleton.initializeGlobalInstance(new AsyncLocalStorage());
```

Without it, `interrupt()` throws `Called interrupt() outside the context of a graph`.

### Custom serializer

The second constructor argument is a `SerializerProtocol`, as with every LangGraph saver. It defaults to LangGraph's `JsonPlusSerializer`.

```ts
new DurableObjectSaver(this.ctx.storage, mySerializer);
```

## What it stores

The saver creates two tables in the object's database:

| Table                  | Holds                                                                     |
| ---------------------- | ------------------------------------------------------------------------- |
| `lc_checkpoints`       | One row per checkpoint: serialized state, metadata, and the parent's id   |
| `lc_checkpoint_writes` | Pending writes from tasks that ran since that checkpoint, in replay order |

Both are created on first use. Tables from an older layout without a `task_path` column are upgraded in place. Avoid using these table names
for your own data.

Every checkpoint stores the complete state, like the official SQLite saver. It doesn't store only the channels that changed.

## Limits

These come from the [Durable Object SQL limits](https://developers.cloudflare.com/durable-objects/platform/limits/):

- **8 MB per checkpoint or write.** A larger state makes `put` or `putWrites` throw. The whole batch rolls back, so nothing partial is stored.
- **10 GB per object** on Workers Paid (1 GB on Free). Checkpoints are never deleted automatically. A long-lived thread keeps every step of
  its history, so call `deleteThread(threadId)` when a conversation ends, or delete the object.

## Security

- **The Durable Object is the security boundary.** Each object has its own database, and one object can't read another's checkpoints.
  Inside an object, `thread_id` only separates conversations; it doesn't grant or check access. If one object serves several users, your code
  must decide which `thread_id` each caller can use.
- **All values are bound as SQL parameters.** Ids, channel names and filter values are never interpolated into SQL. Metadata filters are
  applied in JavaScript after reading, not as SQL JSON paths.
- **Stored data is revived by the serializer.** LangGraph's default `JsonPlusSerializer` doesn't escape plain objects in your state. An
  object shaped like a serialized LangChain object (`{ "lc": 1, "type": "constructor", ... }`) is rebuilt as that class when the checkpoint is
  read, or makes the read throw if the class isn't allowed. Every LangGraph saver behaves the same way. If untrusted input can put
  arbitrary objects into graph state, validate their shape before storing them. Otherwise one crafted value can make that thread impossible
  to load, and also break `list()` calls that span threads. Secret references (`"type": "secret"`) are never resolved from the environment.
  The serializer also keeps an own `__proto__` key from parsed JSON as that object's prototype, so don't trust inherited properties of
  state read back from a checkpoint.
- **Ids must be well-formed strings or safe integers.** Strings with unpaired UTF-16 surrogates, such as user text cut in the middle of an
  emoji, are rejected. SQLite would otherwise store them as a different id, which forks the thread's history.
- **Don't send read errors to clients.** A corrupt row makes reads throw, and JSON parse errors can include a fragment of the stored data.
- `list()` with no `thread_id` returns checkpoints from every thread in the object, matching other savers. An empty string is treated as a
  real thread id, not as "all threads". An empty `checkpoint_id` means "latest", as it does everywhere in LangGraph.

## Development

```sh
npm install
npm test           # runs inside workerd against real Durable Object storage
npm run typecheck
npm run build
```

The tests include LangChain's checkpointer conformance suite (`@langchain/langgraph-checkpoint-validation`). One of its tests is skipped:
it checks an optional optimisation, storing only the channels that changed, which this saver doesn't implement. The official SQLite,
Memory and MongoDB savers skip it too.

## License

MIT
