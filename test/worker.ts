import { DurableObject } from 'cloudflare:workers';
import { DurableObjectSaver } from '../src/index';

/** Owns a saver on its own storage; tests reach it through `runInDurableObject`. */
export class TestObject extends DurableObject {
  private cachedSaver: DurableObjectSaver | undefined;

  get saver(): DurableObjectSaver {
    this.cachedSaver ??= new DurableObjectSaver(this.ctx.storage);
    return this.cachedSaver;
  }
}

export default {
  fetch(): Response {
    return new Response(null, { status: 404 });
  },
};
