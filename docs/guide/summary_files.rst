Catalog Summary Files
===============================================================================

HATS can generate human-readable summary documents for any catalog  
collection.  These are rendered as Markdown (``README.md``) or HTML
(``index.html``) and are suitable for display on platforms such as GitHub,
Hugging Face, or a website.

Documents are produced by rendering `Jinja2 <https://jinja.palletsprojects.com/>`__
templates against a set of context variables derived from the catalog's metadata
and data files.  You can pass a fully custom template string to every generation
function, which lets you control every aspect of the output while still relying
on HATS to gather all the catalog metadata.

Generating a summary file
-------------------------------------------------------------------------------

The main function used is :func:`hats.io.summary_file.write_catalog_summary_file`.
It accepts any HATS catalog path and writes a summary document next to the
catalog data:

.. code-block:: python

    from hats.io.summary_file import write_catalog_summary_file

    write_catalog_summary_file(
        "/path/to/my_catalog",
        fmt="markdown",
    )

The function inspects the catalog type automatically and calls the appropriate
generation function.

**catalog_path**
    Path to the root of any HATS catalog or collection directory.

**fmt**
    ``"markdown"`` writes a ``README.md``; ``"html"`` writes an ``index.html``;
    ``None`` bypasses the built-in templates entirely — requires both
    ``jinja2_template`` and ``filename`` to be provided.

**filename**
    Override the output filename.  Defaults to ``README.md`` or ``index.html``
    depending on ``fmt``.

**output_dir**
    Directory where the output file is written.  Defaults to ``catalog_path``,
    i.e. the summary is placed inside the catalog directory itself.

**name**
    Title used in the document heading.  Defaults to the catalog name from
    ``hats.properties``.

**description**
    Text description rendered below the heading.  A default is
    generated when omitted, but it will be generic ("This is the HATS catalog
    for …").  Providing a real description is strongly recommended for
    public catalogs.

**uri**
    The URI of the catalog (e.g. ``s3://my-bucket/my_catalog`` or an
    HTTPS URL).  Used to build clickable file-structure links and code snippets.
    When ``None``, a ``<PATH>`` placeholder is used in code examples and
    relative links are used elsewhere.

**huggingface_metadata**
    When ``True``, prepends a Hugging Face YAML metadata header to the Markdown
    output.  Only valid when ``fmt="markdown"``.

**jinja2_template**
    A Jinja2 template string **or a path to a** ``.jinja2`` **file** — the file
    is read automatically if the path exists on disk.  When ``None`` the
    built-in default template for the catalog type is used (requires ``fmt`` to
    be ``"markdown"`` or ``"html"``).  See `Custom templates`_ for details.

**extra_template_vars**
    A ``dict`` of additional variables forwarded into ``template.render()``.
    Useful for custom templates that reference fields HATS does not supply by
    default, such as VO registry fields (``accessUrl``, ``shortName``, etc.).
    Ignored when ``None``.

Why output varies between catalogs
-------------------------------------------------------------------------------

Summary documents are generated from *whatever information is available* in the
catalog at the time the function is called.  Several sections are conditionally
included or omitted:

**Column table may be absent or incomplete**
    The column table is built from two sources: a schema read from
    ``dataset/_common_metadata`` (always present in a well-formed catalog),
    and a sample row read from ``dataset/data_thumbnail.parquet`` when it
    exists, or from a randomly-selected partition otherwise.  If neither
    source is accessible, the example-value column is omitted.  Column
    statistics (min, max, null count) are populated only when
    ``hats-import`` computed them during import.

**Sky coverage images may be absent**
    Pixel-map and density-map images require ``matplotlib`` to be installed
    (``pip install hats[visualization]``).  When it is not available, both
    images are silently omitted.

**Cone-search code example**
    A ready-to-run cone-search snippet is included only when the column table
    contains at least one sample value for the RA/Dec columns, so that a
    representative coordinate can be shown.

**Default-column annotations**
    The "Default?" row in the column table appears only when ``hats.properties``
    specifies a ``hats_cols_default`` list.  Catalogs without default columns
    omit this row entirely.

**Nested-column annotations**
    The "Nested?" row appears only when the catalog contains at least one
    ``NestedDtype`` column (a column backed by nested Parquet structs).

**Collection-specific sections**
    Margin and index sub-catalogs are listed only when ``collection.properties``
    records them.  A collection with no margins and no indexes will omit those
    sections.

Custom templates
-------------------------------------------------------------------------------

Pass a Jinja2 template string as ``jinja2_template`` to replace the built-in
template entirely:

.. code-block:: python

    my_template = """
    # {{ name }}
    {{ description }}

    Rows: {{ metadata_table.get("Number of rows", "unknown") }}
    """

    write_catalog_summary_file(
        "/path/to/my_catalog",
        fmt="markdown",
        jinja2_template=my_template,
    )

The template is rendered with
`Jinja2's StrictUndefined <https://jinja.palletsprojects.com/en/stable/api/#jinja2.StrictUndefined>`__,
which means referencing an undefined variable raises an error immediately rather
than silently producing an empty string.  Use ``{% if variable %}`` guards for
optional context variables (see the reference below).

You can also call :func:`hats.io.summary_file.generate_summary` directly if
you only need a rendered string without writing a file.

The built-in templates in ``src/hats/io/templates/`` are a good starting point
for understanding how to use the context variables.

Generating a custom-format file (e.g. VO registry XML)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set ``fmt=None`` to bypass the built-in templates entirely.  HATS still reads
all catalog metadata and makes it available as Jinja2 context variables, but
the output format and filename are fully under your control.  Both
``jinja2_template`` and ``filename`` are required in this mode:

.. code-block:: python

    write_catalog_summary_file(
        "/path/to/my_catalog",
        fmt=None,
        filename="vo-registry.xml",
        output_dir="/path/to/registry/output",
        jinja2_template="/path/to/vo-registry.xml.jinja2",
        extra_template_vars={
            "shortName": "my_catalog",
            "accessUrl": "https://data.lsdb.io/my_catalog",
            "created": "2024-01-01",
        },
    )

The ``extra_template_vars`` dict is unpacked directly into ``template.render()``
alongside the standard HATS context variables (``name``, ``description``,
``cat_props``, etc.), so your template can reference both.

Jinja2 context variable reference
-------------------------------------------------------------------------------

The sections below describe every variable available inside a template.
Variables marked *optional* may be ``None`` or absent for some catalogs —
always guard them with ``{% if variable %}`` before use.

Common variables (all catalog types)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables are present for every catalog type.

* ``name`` *(str)* — title of the document, used in the main heading.
* ``description`` *(str)* — free-text description of the catalog.
* ``uri`` *(str or None)* — canonical URI of the catalog, or ``None`` if not
  provided.
* ``has_partition_info`` *(bool)* — ``True`` when ``partition_info.csv`` exists
  in the catalog directory.
* ``huggingface_metadata`` *(bool)* — whether to include the Hugging Face YAML
  front-matter block.
* ``metadata_table`` *(dict[str, str] or None)* — key/value pairs for the
  catalog metadata table, formatted as strings and ready to render.  Keys
  include "Number of rows", "Number of columns", "Number of partitions",
  "Size on disk", and "HATS Builder" — only those that can be determined are
  present.  ``None`` for index catalogs.
* ``column_table`` *(pandas.DataFrame)* — schema and statistics for every
  column, indexed by column name.  May be an empty DataFrame when schema
  information is unavailable.  See `column_table columns`_ for details.
* ``catalog_dir_name`` *(str)* — directory name of the catalog (e.g.
  ``"my_catalog"``).  Not present for ``CatalogCollection``.
* ``cat_props`` *(TableProperties)* — catalog properties parsed from
  ``hats.properties``.  Commonly used attributes:

  * ``catalog_name`` *(str)* — value of ``obs_collection``.
  * ``catalog_type`` *(CatalogType)* — one of ``OBJECT``, ``SOURCE``,
    ``MARGIN``, ``INDEX``, ``MAP``, or ``ASSOCIATION``.
  * ``total_rows`` *(int or None)* — total row count, or ``None`` if not
    recorded.
  * ``ra_column`` / ``dec_column`` *(str or None)* — names of the spatial
    coordinate columns.
  * ``default_columns`` *(list[str] or None)* — columns loaded by default
    in LSDB, or ``None`` when not specified.
  * ``skymap_order`` *(int or None)* — HEALPix order of ``skymap.fits``.
  * ``skymap_alt_orders`` *(list[int] or None)* — orders of any alternative
    ``skymap.<order>.fits`` files.
  * ``primary_catalog`` *(str or None)* — path to the primary catalog (set
    for margin, index, and association types).
  * ``margin_threshold`` *(float or None)* — margin boundary in arcseconds
    (margin catalogs only).
  * ``indexing_column`` *(str or None)* — column this index is built over
    (index catalogs only).

column_table columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``column_table`` is a ``pandas.DataFrame`` whose row index is the column name.
Not all DataFrame columns are present for every catalog — check with
``"col" in column_table`` before accessing:

* ``dtype`` *(str)* — Apache Arrow data type string (e.g. ``int64``,
  ``float32``, ``large_string``).  Always present.
* ``default`` *(bool)* — whether the column is in the catalog's default-column
  list.  Present only when ``default_columns`` is set on the catalog.
* ``nested_into`` *(str or None)* — name of the parent struct column for nested
  sub-columns, ``None`` for top-level columns.  Present only when the catalog
  has at least one ``NestedDtype`` column.
* ``example`` *(str)* — a formatted sample value from the catalog data.
  Present only when a data file was readable during generation.
* ``min_value`` / ``max_value`` *(str)* — formatted min/max from column
  statistics.  Present only when statistics were computed during import.
* ``rows`` *(str)* — formatted row count per column.  Present only when at
  least one column's count differs from ``cat_props.total_rows`` (e.g. for
  nested columns).
* ``nulls`` *(str)* — formatted null-value count and percentage per column.
  Present only when at least one column contains null values.

CatalogCollection additional variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables are added to the context when generating a summary for a
:class:`~hats.catalog.catalog_collection.CatalogCollection`.

* ``col_props`` *(CollectionProperties)* — collection-level properties from
  ``collection.properties``.  Commonly used attributes:

  * ``name`` *(str)* — value of ``obs_collection``.
  * ``hats_primary_table_url`` *(str)* — relative path to the primary catalog.
  * ``all_margins`` *(list[str] or None)* — relative paths of all margin
    catalogs.
  * ``default_margin`` *(str or None)* — relative path of the default margin.
  * ``all_indexes`` *(dict[str, str] or None)* — maps indexed column name to
    relative path of the index catalog.
  * ``default_index`` *(str or None)* — column name of the default index.

* ``uris`` *(dict)* — structured URI dictionary for all catalogs in the
  collection.  Shape:

  .. code-block:: python

      {
          "collection": str,        # absolute URI of the collection root
          "primary": {
              "name": str,          # relative path of the primary catalog
              "uri":  str,          # absolute URI of the primary catalog
          },
          "margins": [              # default margin listed first
              {"name": str, "uri": str},
              ...
          ],
          "indexes": [              # default index listed first
              {"column": str, "name": str, "uri": str},
              ...
          ],
      }

  When no ``uri`` is supplied to the generation function, ``uris["collection"]``
  is set to the literal string ``"<PATH>"`` and all ``"uri"`` values are
  constructed relative to that placeholder.

* ``margin_thresholds`` *(dict[str, float])* — maps each margin catalog name to
  its threshold in arcseconds.
* ``has_default_columns`` *(bool)* — ``True`` when the primary catalog declares
  default columns.
* ``cone_code_example`` *(dict or None)* — ``{"ra": float, "dec": float}`` with
  a representative sky coordinate for code examples, derived from a sample data
  row.  ``None`` when no sample data is available.
* ``pixel_map_b64`` *(str or None)* — base64-encoded WebP image of the HEALPix
  pixel map.  ``None`` when ``matplotlib`` is not installed.
* ``density_map_b64`` *(str or None)* — base64-encoded WebP image of the
  angular-density sky map.  ``None`` when ``matplotlib`` is not installed.

Catalog additional variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables are added for standalone
:class:`~hats.catalog.catalog.Catalog` summaries (``catalog_type=OBJECT`` or
``SOURCE``):

* ``has_default_columns`` *(bool)* — ``True`` when the catalog declares default
  columns.
* ``cone_code_example`` *(dict or None)* — ``{"ra": float, "dec": float}`` for
  code examples; ``None`` when no sample data is available.
* ``pixel_map_b64`` *(str or None)* — base64-encoded WebP pixel-map image;
  ``None`` without ``matplotlib``.
* ``density_map_b64`` *(str or None)* — base64-encoded WebP density-map image;
  ``None`` without ``matplotlib``.
