# wood-events-service

A minimal, typed FastAPI service template with `uv`, Ruff, mypy, pytest, pre-commit, GitHub Actions, and semantic-release wired together.

## Intended Use

Use this template for small backend HTTP services and internal APIs using FastAPI. The repository infrastructure is ready
for local development and release automation; the package modules are intentionally thin
placeholders for project-specific implementation.

## Project Layout

```text
src/wood_events_service/
  __init__.py
  py.typed
  main.py
  api/
    __init__.py
    router.py
    routes/
      __init__.py
      health.py
  models/
    __init__.py
  services/
    __init__.py
  config.py
  exceptions.py
tests/
  unit/
    test_template_integrity.py
  integration/
Dockerfile
docker-compose.yml
```

Keep reusable Python code under `src/wood_events_service/` and tests under `tests/`.
The `py.typed` marker declares the package as typed.

## Local Setup

Install dependencies into the local environment:

```sh
uv sync --frozen --group dev
```

Install pre-commit hooks:

```sh
uv run pre-commit install
```

Run all baseline checks locally:

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
uv build
docker compose config
uv run pre-commit run --all-files
```

Use Ruff to apply safe fixes:

```sh
uv run ruff check --fix .
uv run ruff format .
```

## Docker Development

Build and run the API container locally:

```sh
docker compose up --build
```

The API listens on `http://localhost:8000`.

The container uses the production runtime. Run development checks with `uv run`
on the host as shown above.

The compose file intentionally includes only the API service. Add databases,
caches, workers, or proxies only when a real project needs them.

## Linting, Formatting, And Typing

Ruff and mypy follow the same conventions as the Python library template:
Python 3.14, `src/` layout, strict mypy, 88-character line length, Ruff import
sorting, and normal `assert` statements allowed in tests.

## Tests

The initial tests verify template integrity without pretending application
behavior exists. Add focused unit and integration tests alongside each real
implementation as the copied project grows.

## Build And Release

The package builds with Hatchling through `uv build`. The shared v3 release
workflow validates mypy, pytest, Docker Compose configuration, and pre-commit,
then builds and verifies the container before python-semantic-release publishes
a Git tag and GitHub Release. Ruff linting and formatting run through pre-commit.

## Automatic dev deployment

Pull requests use centralized validation. After a merge to `main`, the shared
workflow validates and publishes an immutable GHCR image, creates a semantic
release, and opens an infrastructure promotion PR for its digest-qualified
reference.
Infrastructure validates and automatically merges that PR, makes its patch
release, and deploys to dev. The application repository only publishes the
image and requests promotion; infrastructure owns the dev image pin, Compose,
Ansible, runtime secrets, migrations, health checks, and deployment policy.

Before the first deployment, onboard the application in
`SpencerRWood/infrastructure`: add `<app>_image_ref` to `environments/dev.yml`,
the service and Compose definition, Ansible/runtime configuration and secrets,
plus migrations, health checks, and ingress where applicable. Set the initial
image pin to a valid digest-qualified image. This is a separate infrastructure
change; the template does not create it.

Add the generated repository secret `INFRASTRUCTURE_PR_TOKEN`: a fine-grained
token scoped only to `SpencerRWood/infrastructure` with Contents read/write,
Pull requests read/write, Commit statuses read, and Metadata read. Do not
commit the token. The promotion workflow consumes it through its standard
`infrastructure_token` mapping.

## Copy and rename

After creating a repository from this template, run
`python3 scripts/rename_project.py customer-api`, replacing `customer-api`
with your lowercase repository slug. The script reads the existing
`pyproject.toml` project name and updates the package, lockfile, Dockerfile,
release configuration `image_name`, and snake-case `image_key` together. Use
the same slug for the GitHub repository. Then run `uv lock`,
`uv sync --frozen --group dev`,
and the baseline checks.
