# Releasing

Releases are published from GitHub Actions with npm trusted publishing, so no npm token is stored anywhere. Pushing a `v*` tag
stages the version on npm, and a maintainer approves it before it goes live.

You need publish access to `@dklemm/langgraph-checkpoint-durable-object` on npm, with two-factor authentication turned on.

## Cutting a release

From a clean, up-to-date `main`:

```sh
npm version patch        # or minor / major; bumps package.json and creates a git tag
npm pack --dry-run       # the list should be dist/, src/, README.md, LICENSE and package.json only
git push --follow-tags   # the tag starts .github/workflows/release.yml
```

When the Release workflow passes, approve the staged version on npmjs.com under the package's **Staged Packages** tab, or run
`npm stage list` and `npm stage approve <stage-id>`. Both ask for your 2FA code.

The workflow fails without staging anything if the tag doesn't match the version in `package.json`.

## Versioning

Stay on `0.x` until the API has settled. In `0.x`, a minor bump (`0.1.0` → `0.2.0`) may contain breaking changes, and a patch
bump may not.

## Trusted publisher settings

The package's trusted publisher on npmjs.com is GitHub Actions with organization `dklemm`, repository
`langgraph-checkpoint-durable-object`, workflow `release.yml` and environment `npm`. Only `npm stage publish` is allowed. If you
rename the workflow file or the environment, delete the trusted publisher and create a new one, because npm can't edit it.

On GitHub, the `npm` environment only accepts deployments from `v*` tags.

## Why `publishConfig.access` is set

npm makes scoped packages private by default. `"publishConfig": { "access": "public" }` in `package.json` publishes this one
publicly.
