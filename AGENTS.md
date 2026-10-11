# Events Service

## Branching strategy

- This repository uses **release-branch integration**. Create Story branches from the verified active `release/vX.Y.Z` and target Story PRs there; `main` changes only after the release readiness gate.
- Follow the [canonical branching and delivery policy](https://github.com/SpencerRWood/codex-config/blob/main/docs/branching-policy.md), available through the installed codex-config source path. Verify the strategy, release association, fetched base and PR target before implementation and record them in Story evidence. Story completion does not authorize publication or deployment; retain existing review, CI, security and rollback requirements.

Follow the active R1 Story and revised requirements. Keep one Story per branch.
Keep wood-broker and wood-notify independently runnable; infrastructure owns
production topology, external PostgreSQL, secret injection, and promotion.
Do not persist or log credentials. Provider adapters and routing are later Stories.
Use Wood Tools for validation and repository-owned verification.
