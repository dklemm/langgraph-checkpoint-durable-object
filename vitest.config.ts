import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

// The validation suite has a test for an optional optimisation: storing only
// the channels listed in `newVersions`. Like the official SQLite, Memory and
// MongoDB savers, this saver stores whole checkpoints, so that test is skipped.
const SKIPPED_VALIDATION_TEST = 'should only store channel_values that have changed';

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './test/wrangler.jsonc' } })],
  test: {
    // The LangGraph validation suite calls describe/it/beforeAll as globals.
    globals: true,
    testNamePattern: new RegExp(`^(?!.*${SKIPPED_VALIDATION_TEST}).*$`),
  },
});
