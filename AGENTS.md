## Development lifecycle

Use root [tool.wood.branching]: feature branches and Story PRs start from and target development. Development validates without publication/deployment. Main requires a separately approved release PR and pinned release workflow. Emergency hotfixes require incident/approval references and reconciliation into development.

# Events Service

Follow the active R1 Story and revised requirements. Keep one Story per branch.
Keep wood-broker and wood-notify independently runnable; infrastructure owns
production topology, external PostgreSQL, secret injection, and promotion.
Do not persist or log credentials. Provider adapters and routing are later Stories.
Use Wood Tools for validation and repository-owned verification.
