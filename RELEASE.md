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

The release action will do the following:

1. Verify that the version adheres to the semantic versioning format.
2. Check out the latest code from the `main` branch.
3. Update the version in Cargo.toml to the specified version.
4. Commit the changes and push to the `main` branch.
5. Create a Github release with the specified version and auto-generated release notes.
6. Publish a release to crates.io.

### Patch releases

To create a patch release for an existing version:

1. If it doesn't already exist, create a <major>.<minor>.x branch from the release tag. So for example, for the v0.6.0 release, run `git checkout -b v0.6.x v0.6.0`.
2. Cherry-pick the desired changes onto the release branch and push it.
3. Run the release workflow against the release branch and specify the desired release version (e.g. v0.6.1)

### Bindings

#### Python Bindings

SlateDB Python bindings are published using a Github release action defined in `.github/workflows/python.yml`. To create a new release:

1. Go to the [python release action page](https://github.com/slatedb/slatedb/actions/workflows/python.yaml)
2. Input a version value in the format `X.Y.Z` and click `Run workflow`.

Python releases can only run after a crate release has been published to crates.io using the Rust publication process shown above. This is because the Python release action can only run against a release tag in the git repo (not on main).

#### Java Bindings (Maven Central)

SlateDB Java bindings are published using the Github release action defined in `.github/workflows/java.yaml`.

Before publishing Java artifacts, configure these repository secrets:

1. `MAVEN_CENTRAL_USERNAME`: Sonatype Central Portal token username.
2. `MAVEN_CENTRAL_PASSWORD`: Sonatype Central Portal token password.
3. `MAVEN_CENTRAL_SIGNING_KEY`: ASCII-armored private GPG key used to sign artifacts.
4. `MAVEN_CENTRAL_SIGNING_KEY_PASSWORD`: Passphrase for the signing key.

To create a Java release:

1. Ensure the Rust release exists and the `vX.Y.Z` tag has been created.
2. Go to the [java release action page](https://github.com/slatedb/slatedb/actions/workflows/java.yaml).
3. Input the same version value used for the tag (format `X.Y.Z`) and click `Run workflow`.

The Java release action will do the following:

1. Verify the version string follows semantic versioning.
2. Build native `slatedb-c` libraries for all supported targets:
   - `linux-x86_64`
   - `linux-aarch64`
   - `macos-x86_64`
   - `macos-aarch64`
   - `windows-x86_64`
   - `windows-aarch64`
3. Assemble one universal Java artifact containing all native libraries.
4. Publish signed Maven artifacts (`jar`, `sources`, `javadoc`, `pom`) to Maven Central.
