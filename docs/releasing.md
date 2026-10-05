# Releasing

1. Update the version in `pyproject.toml`, run `uv lock`, and describe user-visible changes
   in the release notes. Use conventional commits.
2. Run `task check`, `task build`, and `uvx twine check --strict dist/*`.
3. Push the commit and wait for every CI job. Create a matching `vX.Y.Z` tag at that commit.
4. Publish a GitHub release at that tag, attaching its wheel and source distribution.
   The `Publish` workflow repeats CI, checks the tag matches the package version, builds
   distributions, and publishes them to PyPI with GitHub OIDC and attestations.
5. Verify the PyPI version and install it in an isolated environment.

The PyPI trusted publisher must specify owner `binarylogic`, repository `py-wftnp`,
workflow `publish.yml`, and environment `publish`. The first publication needs a pending
publisher for project `wftnp` configured in the owner's PyPI account. No API token is needed.
The workflow can be dispatched on a release tag to retry an interrupted publication.
PyPI versions are immutable; do not reuse a published version for changed artifacts.
