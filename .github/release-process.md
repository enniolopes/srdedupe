# Release Process

## Quick Steps

```bash
# 1. Create release branch
git checkout -b release/vX.X.X

# 2. Update version in pyproject.toml
# Change: version = "X.X.X"

# 3. Commit and push
git add pyproject.toml
git commit -m "chore: bump version to X.X.X"
git push origin release/vX.X.X

# 4. Create PR on GitHub, wait for CI, then merge

# 5. Tag the merge commit
git checkout main
git pull origin main
git tag -a vX.X.X -m "Release vX.X.X"
git push origin vX.X.X
```

## Troubleshooting

CI fails on PR: Fix errors, commit, push to same branch

Forgot to tag: Create tag on the merge commit and push it

Wrong tag: Delete and recreate

```bash
git tag -d vX.X.X
git push origin :refs/tags/vX.X.X
```
