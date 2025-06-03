# Releasing SlateDB

## Cadence

We aim to release a new version of SlateDB every 2 months. This is a guideline, not a strict rule. The actual cadence may vary based on the amount of work that has been completed.

## Proposal

Anyone may propose a new SlateDB release by opening a Github issue.

## Approval

[GOVERNANCE.md](GOVERNANCE.md) defines how a release is approved.

## Publication

SlateDB releases are published using a Github release action defined in `.github/workflows/release.yml`. To create a new release:

1. Go to the [release action page](https://github.com/slatedb/slatedb/actions/workflows/release.yaml)
2. Input a version value in the format `X.Y.Z` and click `Run workflow`.

 To create a patch release for an existing version:

1. If it doesn't already exist, create a <major>.<minor>.x branch from the release tag. So for example, for the v0.6.0 release, run `git checkout -b 0.6.x v0.6.0`.
2. Cherry-pick the desired changes onto the release branch and push it.
3. Run the release workflow against the release branch and specify the desired release version (e.g. v0.6.1)

The release action will do the following:

1. Verify that the version adheres to the semantic versioning format.
2. Check out the latest code from the `main` branch.
3. Update the version in Cargo.toml to the specified version.
4. Commit the changes and push to the `main` branch.
5. Create a Github release with the specified version and auto-generated release notes.
6. Publish a release to crates.io.
