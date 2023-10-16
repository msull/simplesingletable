from invoke import task, Context


@task
def compile_requirements(c: Context):
    c.run("pip-compile --extra dev pyproject.toml")
