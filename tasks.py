from pathlib import Path

from invoke import Context, task


def from_repo_root(c: Context):
    return c.cd(Path(__file__).parent)


@task
def compile_requirements(c: Context, install=True):
    with from_repo_root(c):
        c.run("pip-compile --extra dev --extra build pyproject.toml", pty=True)
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
        c.run("ruff src/ tasks.py --fix")
