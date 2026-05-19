import re
from datetime import date as _date
from pathlib import Path
from time import sleep

from invoke import Context, task


class Paths:
    repo_root = Path(__file__).parent
    example_tables = repo_root / "example_tables"
    changelog = repo_root / "CHANGELOG.md"
    pyproject = repo_root / "pyproject.toml"


def from_repo_root(c: Context):
    return c.cd(Paths.repo_root)


def _read_current_version() -> str:
    text = Paths.pyproject.read_text()
    # Read from the bumpver-owned line so we always agree with bumpver.
    match = re.search(r'^current_version = "([^"]+)"', text, flags=re.MULTILINE)
    if not match:
        raise SystemExit("Could not find [tool.bumpver].current_version in pyproject.toml")
    return match.group(1)


def _compute_new_version(current: str, major: bool, minor: bool, patch: bool) -> str:
    parts = current.split(".")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise SystemExit(f"Unexpected version format (expected MAJOR.MINOR.PATCH): {current!r}")
    M, m, p = (int(x) for x in parts)
    if major:
        return f"{M + 1}.0.0"
    if minor:
        return f"{M}.{m + 1}.0"
    if patch:
        return f"{M}.{m}.{p + 1}"
    raise SystemExit("Must specify exactly one of --major, --minor, --patch")


@task
def compile_requirements(c: Context, install=True, upgrade=False):
    with from_repo_root(c):
        upgrade_flag = "--upgrade" if upgrade else ""
        c.run(f"pip-compile {upgrade_flag} -v --strip-extras --extra dev --extra build pyproject.toml", pty=True)
        c.run('echo "-e ." >> requirements.txt')
        if install:
            c.run("pip-sync", pty=True)


@task
def bumpver(c: Context, major=False, minor=False, patch=False, dry=False):
    num_set = 0
    flag = ""
    if major:
        flag = "--major"
        num_set += 1
    if minor:
        flag = "--minor"
        num_set += 1
    if patch:
        flag = "--patch"
        num_set += 1
    if num_set != 1:
        raise SystemExit("Must specify exactly one of --major, --minor, --patch")
    with from_repo_root(c):
        dry_flag = ""
        if dry:
            dry_flag = "--dry"
        c.run(f"bumpver update {flag} {dry_flag}", pty=True)


@task
def build(c: Context, clean=True):
    with from_repo_root(c):
        if clean:
            c.run("rm -rf dist/*")
        c.run("python -m build")
        c.run("twine check dist/*")


@task
def publish(c: Context, testpypi=True):
    if testpypi:
        testpypi_flag = "-r testpypi"
    else:
        testpypi_flag = ""
    with from_repo_root(c):
        c.run(f"twine upload {testpypi_flag} dist/*", pty=True)


@task
def lint(c: Context):
    with from_repo_root(c):
        c.run("black src/ tasks.py")
        c.run("isort src/ tasks.py")
        c.run("ruff check src/ tasks.py --fix")


@task
def launch_dynamodb_local(c: Context, create_tables=False, clear_data=False):
    """Run local dynamodb, with options to wipe data and create a table with required indices."""
    with from_repo_root(c):
        c.run("docker stop dynamodb-local || true", hide="both")
        if clear_data:
            c.run("rm -rf $(pwd)/local/dynamodb")
        c.run(
            "docker run --rm -d --name dynamodb-local -p 8000:8000 -v "
            "$(pwd)/local/dynamodb:/data/ amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /data"
        )
        if create_tables:
            sleep(1)
            for file in Paths.example_tables.iterdir():
                c.run(
                    f"AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=unused AWS_SECRET_ACCESS_KEY=unused "
                    f"aws dynamodb create-table --cli-input-yaml file://{file} --endpoint-url http://localhost:8000"
                )


@task
def halt_dynamodb_local(c: Context):
    """Run local dynamodb, with options to wipe data and create a table with required indices."""
    c.run("docker stop dynamodb-local || true", hide="both")


@task
def run_streamlit_app(c: Context):
    with from_repo_root(c):
        c.run("streamlit run ./src/streamlit_app/streamlit_app.py --server.headless True", pty=True)


@task
def stamp_changelog(c: Context, version: str, release_date: str = ""):
    """Convert the `## [Unreleased]` section into a dated release header for `version`.

    Leaves an empty `## [Unreleased]` section above the new one so the next cycle can
    accumulate entries in the same place.
    """
    if not release_date:
        release_date = _date.today().isoformat()
    text = Paths.changelog.read_text()
    marker = "## [Unreleased]"
    if marker not in text:
        raise SystemExit(f"CHANGELOG.md is missing a '{marker}' section")
    new_header = f"{marker}\n\n## [{version}] {release_date}"
    Paths.changelog.write_text(text.replace(marker, new_header, 1))
    print(f"Stamped CHANGELOG.md with [{version}] {release_date}")


@task
def fullrelease(c: Context, major=False, minor=False, patch=False):
    lint(c)
    with from_repo_root(c):
        c.run("pytest", pty=True)
    new_version = _compute_new_version(_read_current_version(), major, minor, patch)
    stamp_changelog(c, new_version)
    with from_repo_root(c):
        c.run("git add CHANGELOG.md", pty=True)
        c.run('git commit -m "update CHANGELOG for release"', pty=True)
    bumpver(c, major, minor, patch)
    build(c)
    publish(c, testpypi=False)
    c.run("git push", pty=True)
    c.run("git push --tags", pty=True)
