from pathlib import Path

from invoke import Context, task


def from_repo_root(c: Context):
    return c.cd(Path(__file__).parent)


@task
def compile_requirements(c: Context):
    with from_repo_root(c):
        c.run("pip-compile --extra dev pyproject.toml")


@task
def install_build_requirements(c: Context):
    with from_repo_root(c):
        c.run("python -m pip install build twine")


@task
def build(c: Context):
    with from_repo_root(c):
        c.run("python -m build")
        c.run("twine check dist/*")


@task
def publish(c: Context, testpypi=True):
    if testpypi:
        testpypi_flag = "-r testpypi"
    else:
        testpypi_flag = ""
    with from_repo_root(c):
        c.run(f"twine upload {testpypi_flag} dist/*")


@task
def lint(c: Context):
    with from_repo_root(c):
        c.run("black src/ tasks.py")
        c.run("isort src/ tasks.py")
        c.run("ruff src/ tasks.py --fix")
