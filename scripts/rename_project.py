"""Rename a copied service template from its existing project name."""

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache"}


def main(slug: str) -> None:
    """Replace the current project slug and matching Python package name."""
    if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", slug):
        raise ValueError("Expected a lowercase, hyphenated repository slug")

    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    old_slug = pyproject["project"]["name"]
    old_package = old_slug.replace("-", "_")
    new_package = slug.replace("-", "_")
    if old_slug == slug:
        return

    paths = sorted(
        (path for path in ROOT.rglob("*") if not SKIP.intersection(path.parts)),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in paths:
        if not path.is_file():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = content.replace(old_slug, slug).replace(old_package, new_package)
        if updated != content:
            path.write_text(updated, encoding="utf-8")

    for path in paths:
        if not path.exists():
            continue
        new_name = path.name.replace(old_slug, slug).replace(old_package, new_package)
        if new_name != path.name:
            path.rename(path.with_name(new_name))


if __name__ == "__main__":
    main(sys.argv[1])
