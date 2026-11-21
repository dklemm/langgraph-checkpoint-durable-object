import { validate } from '@langchain/langgraph-checkpoint-validation';
import { ForwardingSaver, newObject } from './helpers';

// LangChain's conformance suite for checkpoint savers. Each run gets a fresh
// Durable Object, so state never leaks between suites.
validate<ForwardingSaver>({
  checkpointerName: '@dklemm/langgraph-checkpoint-durable-object',
  createCheckpointer: () => new ForwardingSaver(newObject()),
});
