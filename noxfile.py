"""Automation using nox."""

import glob
import os

import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = "lint", "tests"
locations = "src", "tests"

pip_dev_flags = ["--use-pep517"]  # reflink package is still missing wheels

project = nox.project.load_toml()
python_versions = nox.project.python_versions(project)
pypy_versions = ["pypy3.9", "pypy3.10", "pypy3.11"]


@nox.session(python=python_versions + pypy_versions)
def tests(session: nox.Session) -> None:
    session.install(".[tests]", *pip_dev_flags)
    session.run(
        "pytest",
        "--cov",
        "--cov-config=pyproject.toml",
        *session.posargs,
        env={"COVERAGE_FILE": f".coverage.{session.python}"},
    )


@nox.session
def bench(session: nox.Session) -> None:
    session.install(".[tests]")
    session.run("pytest", "--benchmark-only", *session.posargs)


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.install("-e", ".[dev]", *pip_dev_flags)

    args = *(session.posargs or ("--show-diff-on-failure",)), "--all-files"
    session.run("pre-commit", "run", *args)
    session.run("python", "-m", "mypy")


@nox.session
def build(session: nox.Session) -> None:
    session.install("build", "setuptools", "twine")
    session.run("python", "-m", "build")
    dists = glob.glob("dist/*")
    session.run("twine", "check", *dists, silent=True)


@nox.session
def dev(session: nox.Session) -> None:
    """Sets up a python development environment for the project."""
    args = session.posargs or ("venv",)
    venv_dir = os.fsdecode(os.path.abspath(args[0]))

    session.log(f"Setting up virtual environment in {venv_dir}")
    session.install("virtualenv")
    session.run("virtualenv", venv_dir, silent=True)

    python = os.path.join(venv_dir, "bin/python")
    session.run(
        python, "-m", "pip", "install", "-e", ".[dev]", *pip_dev_flags, external=True
    )
