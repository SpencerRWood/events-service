"""Check the rendered service workflow, not only its template source."""

import runpy
import shutil
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_generated_customer_api_deployment(tmp_path: Path) -> None:
    project = tmp_path / "customer-api"
    shutil.copytree(
        ROOT,
        project,
        ignore=shutil.ignore_patterns(
            ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache"
        ),
    )
    runpy.run_path(str(project / "scripts/rename_project.py"))["main"]("customer-api")

    metadata = tomllib.loads((project / "pyproject.toml").read_text())
    release_config = tomllib.loads((project / ".github/release.toml").read_text())
    assert metadata["project"]["name"] == "customer-api"
    assert (project / "src/customer_api/main.py").is_file()
    assert release_config["container"] == {
        "publish": True,
        "image_name": "customer-api",
    }
    workflow = (project / ".github/workflows/release.yml").read_text()
    parsed = yaml.safe_load(workflow)
    jobs = parsed["jobs"]
    assert jobs["release"]["uses"].endswith("/release-container.yml@v3")
    assert jobs["release"]["permissions"] == {
        "contents": "write",
        "packages": "write",
    }
    assert "container" not in jobs
    assert jobs["promotion"]["uses"].endswith("/promote-container-to-dev.yml@v2")
    assert jobs["promotion"]["with"]["image_name"] == "customer-api"
    assert jobs["promotion"]["with"]["image_key"] == "customer_api_image_ref"
    assert jobs["promotion"]["with"]["version_image_digest"] == (
        "${{ needs.release.outputs.version_image_digest }}"
    )
    assert (
        "INFRASTRUCTURE_PR_TOKEN"
        in jobs["promotion"]["secrets"]["infrastructure_token"]
    )
    assert yaml.safe_load((project / ".github/workflows/validate.yml").read_text())[
        "jobs"
    ]["validation"]["uses"].endswith("/validate.yml@v3")
    for forbidden in (
        "environment_file",
        "promotion_branch_prefix",
        "pr_title_template",
        "gh pr",
        ":latest",
    ):
        assert forbidden not in workflow
