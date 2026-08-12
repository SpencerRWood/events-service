# Wood Events Service Agent Instructions

Implementation must follow the release implementation workbook/story being executed.

Use one implementation story per feature branch. Code must not be added outside the active story scope.

`wood-broker` and `wood-notify` must remain independently deployable.

ntfy is bundled deployment infrastructure, but notification delivery must remain adapter-backed and replaceable by configuration.

PostgreSQL may be external shared infrastructure and must not be assumed to be bundled in the production Compose stack.

Secrets must never be committed to this repository.
