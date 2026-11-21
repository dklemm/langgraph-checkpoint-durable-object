# Releasing

You need publish access to `@dklemm/langgraph-checkpoint-durable-object` on npm, with two-factor authentication turned on, and
you need to be logged in (`npm login`).

## Cutting a release

From a clean `main`:

```sh
npm version patch        # or minor / major; bumps package.json and creates a git tag
npm pack --dry-run       # the list should be dist/, src/, README.md, LICENSE and package.json only
npm publish              # runs typecheck, tests and build first, then asks for your 2FA code
git push --follow-tags
```

## Versioning

Stay on `0.x` until the API has settled. In `0.x`, a minor bump (`0.1.0` → `0.2.0`) may contain breaking changes, and a patch
bump may not.

## Why `publishConfig.access` is set

npm makes scoped packages private by default. `"publishConfig": { "access": "public" }` in `package.json` publishes this one
publicly.
