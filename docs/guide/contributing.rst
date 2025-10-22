Contributing to hats
===============================================================================

Find (or make) a new GitHub issue
-------------------------------------------------------------------------------

Add yourself as the assignee on an existing issue so that we know who's working 
on what. If you're not actively working on an issue, unassign yourself.

If there isn't an issue for the work you want to do, please create one and include
a description.

You can reach the team with bug reports, feature requests, and general inquiries
by creating a new GitHub issue.

Create a branch
-------------------------------------------------------------------------------

It is preferable that you create a new branch with a name like 
``issue/##/<short-description>``. GitHub makes it pretty easy to associate 
branches and tickets, but it's nice when it's in the name.

Set up a development environment
-------------------------------------------------------------------------------

Before installing any dependencies or writing code, it's a great idea to create a
virtual environment. LINCC-Frameworks engineers primarily use `conda` to manage virtual
environments. If you have conda installed locally, you can run the following to
create and activate a new environment.

.. code-block:: bash

   >> conda create env -n <env_name> python=3.11
   >> conda activate <env_name>


Once you have created a new environment, you can install this project for local
development using the following commands:

.. code-block:: bash

   >> pip install -e .'[dev]'
   >> pre-commit install
   >> conda install pandoc


Notes:

1) The single quotes around ``'[dev]'`` may not be required for your operating system.
2) ``pre-commit install`` will initialize pre-commit for this local repository, so
   that a set of tests will be run prior to completing a local commit. For more
   information, see the Python Project Template documentation on
   `pre-commit <https://lincc-ppt.readthedocs.io/en/stable/practices/precommit.html>`__.
3) Install ``pandoc`` allows you to verify that automatic rendering of Jupyter notebooks
   into documentation for ReadTheDocs works as expected. For more information, see
   the Python Project Template documentation on
   `Sphinx and Python Notebooks <https://lincc-ppt.readthedocs.io/en/stable/practices/sphinx.html#python-notebooks>`__.

.. tip::
    Installing on Mac
       
    When installing dev dependencies, make sure to include the single quotes.

    .. code-block:: bash
        
        $ pip install -e '.[dev]'

Testing
-------------------------------------------------------------------------------

Please add or update unit tests for all changes made to the codebase. You can run
unit tests locally simply with:

.. code-block:: bash

    pytest


.. tip::
    While developing tests, it can be helpful to run only specific test method(s), 
    as demonstrated in the 
    `pytest documentation <https://docs.pytest.org/en/stable/how-to/usage.html#specifying-which-tests-to-run>`__. 
    However, the before committing, the full test suite should be run to ensure the new changes 
    to not break existing functionality.

.. tip::
    If there are unexpected pytest failures that suggest removing pycache folders/files, 
    this can often be resolved by removing the pycache folders/files for the submodule(s) 
    that are being modified/developed.


If you're making changes to the sphinx documentation (anything under ``docs``),
you can build the documentation locally with a command like:

.. code-block:: bash

    cd docs
    make html

We also have a handful of automated linters and checks using ``pre-commit``. You
can run against all staged changes with the command:

.. code-block:: bash

    pre-commit

.. admonition:: Staging changes

    Changes can be staged by running the following within 
    the repository directory: 

    .. code-block:: bash
        
        $ git add -A

    In many cases, linting changes can be made automatically, which can 
    be verified by rerunning the staging & ``pre-commit`` steps again.

.. admonition:: ``pre-commit`` python version mismatch

    If the python version within your development environment does not 
    match the specified ``pre-commit`` ``black-jupyter`` hook language version 
    (in the ``.pre-commit-config.yaml`` file), ``pre-commit`` will fail.

    To solve this, the ``language_version`` variable can be temporarily changed. 
    ``pre-commit`` can then be run as needed. After finishing, 
    this variable should be reset to its original value. 
    Commiting changes will then require bypassing ``pre-commit`` (see below).

.. admonition:: Bypassing ``pre-commit``

    In some cases, there can be problems committing due to re-commit failures 
    that are unrelated to the staged changes (e.g., problems building an 
    unchanged part of the documentation).

    If the full test suite runs successfully, the ``pre-commit`` checks 
    can be skipped by running:

    .. code-block:: bash

         $ git commit -m "Commit message" --no-verify

    (These checks will still be run in the github CI/CD pipeline. 
    Thus, this bypass should only be used when necessary, 
    as resolving issues locally ensures successful CI/CD checks.)


Create your PR
-------------------------------------------------------------------------------

Please use PR best practices, and get someone to review your code.

We have a suite of continuous integration tests that run on PR creation. Please
follow the recommendations of the linter.

Merge your PR
-------------------------------------------------------------------------------

The author of the PR is welcome to merge their own PR into the repository.

Optional - Release a new version
-------------------------------------------------------------------------------

Once your PR is merged you can create a new release to make your changes available. 
GitHub's `instructions <https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository>`__ 
for doing so are here. 
Use your best judgement when incrementing the version. i.e. is this a major, minor, or patch fix.