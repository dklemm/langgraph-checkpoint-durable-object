import type { TestObject } from './worker';

declare global {
  namespace Cloudflare {
    interface Env {
      TEST_OBJECT: DurableObjectNamespace<TestObject>;
    }
  }
}
