Getting Started with HATS
=========================

Installation
------------

The latest release version of HATS is available to install with `pip <https://pypi.org/project/hats/>`_ (with conda coming soon).

.. code-block:: bash

    python -m pip install hats

.. hint::

    We recommend using a virtual environment. Before installing the package, create and activate a fresh
    environment. Here are some examples with different tools:

    .. tab-set::

        .. tab-item:: venv

            .. code-block:: bash

                python -m venv ./hats_env
                source ./hats_env/bin/activate

        .. tab-item:: pyenv

            With the pyenv-virtualenv plug-in:

            .. code-block:: bash

                pyenv virtualenv 3.11 hats_env
                pyenv local hats_env

    We recommend Python versions **>=3.9, <=3.12**.

HATS can also be installed from source on `GitHub <https://github.com/astronomy-commons/hats>`_.


LSDB
----

For the most part, we recommend accessing and processing HATS data using the `LSDB package
<https://github.com/astronomy-commons/lsdb>`_ framework. LSDB provides a variety of utility
functions as well as a lazy, distributed execution framework using Dask.

For detail on LSDB, see the `readthedocs site <https://docs.lsdb.io/en/stable/>`_.
