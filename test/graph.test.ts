import { AsyncLocalStorage } from 'node:async_hooks';
import { AsyncLocalStorageProviderSingleton } from '@langchain/core/singletons';
import { Annotation, Command, END, START, Send, StateGraph, interrupt } from '@langchain/langgraph';
import type { BaseCheckpointSaver } from '@langchain/langgraph-checkpoint';
import { evictDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { collect, inObject, newObject } from './helpers';

// LangGraph needs real AsyncLocalStorage for interrupt(); Workers get it via nodejs_compat.
AsyncLocalStorageProviderSingleton.initializeGlobalInstance(new AsyncLocalStorage());

const LogState = Annotation.Root({
  log: Annotation<string[]>({ reducer: (current, update) => current.concat(update), default: () => [] }),
});

type LogUpdate = typeof LogState.Update;

const threadConfig = { configurable: { thread_id: 'call-1' } };

function interruptingGraph(checkpointer: BaseCheckpointSaver) {
  return new StateGraph(LogState)
    .addNode('greet', (): LogUpdate => ({ log: ['greeted'] }))
    .addNode('ask', (): LogUpdate => ({ log: [`name is ${interrupt('What is your name?')}`] }))
    .addEdge(START, 'greet')
    .addEdge('greet', 'ask')
    .addEdge('ask', END)
    .compile({ checkpointer });
}

describe('with a real LangGraph graph', () => {
  it('resumes an interrupted run after the Durable Object is evicted', async () => {
    const stub = newObject();

    const paused = await inObject(stub, async (saver) => {
      const graph = interruptingGraph(saver);
      await graph.invoke({ log: [] }, threadConfig);
      return graph.getState(threadConfig);
    });
    expect(paused.next).toEqual(['ask']);
    expect(paused.tasks[0]?.interrupts[0]?.value).toBe('What is your name?');

    await evictDurableObject(stub);

    const finished = await inObject(stub, async (saver) => {
      const graph = interruptingGraph(saver);
      const result = await graph.invoke(new Command({ resume: 'Ada' }), threadConfig);
      const history = await collect(graph.getStateHistory(threadConfig));
      return { result, historyLength: history.length };
    });
    expect(finished.result.log).toEqual(['greeted', 'name is Ada']);
    expect(finished.historyLength).toBeGreaterThan(2);
  });

  it('fans out with Send and merges results in a stable order', async () => {
    const result = await inObject(newObject(), (saver) => {
      const WorkState = Annotation.Root({
        items: Annotation<string[]>(),
        log: Annotation<string[]>({ reducer: (current, update) => current.concat(update), default: () => [] }),
      });
      const ItemState = Annotation.Root({ item: Annotation<string>() });
      const graph = new StateGraph(WorkState)
        .addNode('work', (state: typeof ItemState.State) => ({ log: [state.item.toUpperCase()] }))
        .addConditionalEdges(START, (state: typeof WorkState.State) => state.items.map((item) => new Send('work', { item })))
        .addEdge('work', END)
        .compile({ checkpointer: saver });
      return graph.invoke({ items: ['a', 'b', 'c', 'd'] }, threadConfig);
    });
    expect(result.log).toEqual(['A', 'B', 'C', 'D']);
  });

  it('keeps a finished sibling’s writes when a parallel node fails, so it is not rerun', async () => {
    const counts = { steady: 0, flaky: 0 };
    const outcome = await inObject(newObject(), async (saver) => {
      const graph = new StateGraph(LogState)
        .addNode('steady', (): LogUpdate => {
          counts.steady++;
          return { log: ['steady'] };
        })
        .addNode('flaky', (): LogUpdate => {
          counts.flaky++;
          if (counts.flaky === 1) throw new Error('transient failure');
          return { log: ['flaky'] };
        })
        .addEdge(START, 'steady')
        .addEdge(START, 'flaky')
        .addEdge('steady', END)
        .addEdge('flaky', END)
        .compile({ checkpointer: saver });

      await expect(graph.invoke({ log: [] }, threadConfig)).rejects.toThrow('transient failure');
      // Resume from the last checkpoint: only the failed node should run again.
      return graph.invoke(null, threadConfig);
    });
    expect(counts).toEqual({ steady: 1, flaky: 2 });
    expect([...outcome.log].sort()).toEqual(['flaky', 'steady']);
  });
});
