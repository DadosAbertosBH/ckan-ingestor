---
name: git-release-tag
description: Create and push the next patch-level semantic-version Git tag for a repository when the user requests a release tag or asks to tag and push the current commit.
---

# Git Release Tag

Use this skill for an explicitly authorized release-tag operation. It handles the tag and push; it does not create a release or publish binaries.

1. Inspect the current branch, `HEAD`, remote configuration, worktree status, and existing tags.
2. Find the highest existing tag matching `vMAJOR.MINOR.PATCH` (also accept the same format without `v`). If the user supplied a tag name, use it after validating that it is a valid, unused semantic-version tag.
3. When the user asks for “the next” tag, increment the patch component of the highest matching tag. If no semantic-version tag exists, ask for the initial version instead of guessing.
4. Report the exact tag, commit SHA, branch, and any uncommitted changes before mutating the remote. Continue only when the user’s request clearly authorizes creating and pushing the tag; ask for confirmation when the requested version or target is ambiguous.
5. Ensure the tag does not already exist locally or on `origin`. Create an annotated tag with a concise message such as `Release v1.2.3`, then push that exact tag to the configured origin.
6. Verify the pushed tag resolves to the intended commit and report the tag, commit, and remote.

Do not rewrite, delete, or move existing tags. Do not commit unrelated work. A dirty worktree is informational because tags reference `HEAD`; surface it clearly and stop if the user’s requested target is unclear. Use repository-local conventions when they specify a different tag prefix or versioning scheme.
