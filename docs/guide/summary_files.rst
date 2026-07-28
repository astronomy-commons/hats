Catalog Summary Files
===============================================================================

Every HATS catalog or collection can generate its own human-readable summary
document, a ``README.md`` or an ``index.html``,  so it's immediately
presentable on GitHub, Hugging Face, or any static site, without you having to
write that page by hand.

Under the hood, this works by rendering a `Jinja2 <https://jinja.palletsprojects.com/>`__
template against a set of context variables that HATS pulls from the catalog's
metadata and data files.  If the built-in layout isn't what you want, you can
hand in your own template string instead, and HATS will still do the work of
gathering the metadata for you: you just control how it's displayed.

Generating a summary file
-------------------------------------------------------------------------------

The main function you'll use is :func:`hats.io.summary_file.write_catalog_summary_file`.
Point it at any HATS catalog path and it writes a summary document right next
to the catalog data:

.. code-block:: python

    from hats.io.summary_file import write_catalog_summary_file

    write_catalog_summary_file(
        "/path/to/my_catalog",
        fmt="markdown",
    )

It figures out the catalog type on its own — catalog, collection, margin,
index, or association,and calls the right generation function accordingly.

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
    built-in template for the catalog type is used (requires ``fmt`` to
    be ``"markdown"`` or ``"html"``).  See `Custom templates`_ for details.

**extra_template_vars**
    A ``dict`` of additional variables forwarded into ``template.render()``.
    Useful for custom templates that reference fields HATS does not supply by
    default, such as VO registry fields (``accessUrl``, ``shortName``, etc.).
    Ignored when ``None``.

Why output varies between catalogs
-------------------------------------------------------------------------------

Summary documents only describe *whatever's actually there* when the function
runs, so depending on the catalog, some sections show up and others don't.
Here's why:

**Column table may be absent or incomplete**
    The column table draws from two sources: the schema in
    ``dataset/_common_metadata`` (which a well-formed catalog always has),
    and a sample row pulled from ``dataset/data_thumbnail.parquet`` if it
    exists, or a randomly-selected partition if it doesn't.  When neither
    source is reachable, the example-value column just gets left out.  Column
    statistics like min, max, and null count only show up if ``hats-import``
    actually computed them at import time.

**Sky coverage images may be absent**
    Pixel-map and density-map images need ``matplotlib`` installed to render
    at all.  Without it, both images are quietly left out — and for margin
    and index catalogs, they're skipped regardless, since those catalog
    types don't get sky-coverage images in the first place.

**Cone-search code example**
    You'll only see a ready-to-run cone-search snippet when the column table
    has at least one sample RA/Dec value to work with: that's what lets
    HATS show a real, representative coordinate.  Association catalogs never
    declare RA/Dec columns to begin with, so this snippet never shows up for
    them.

**Default-column annotations**
    The "Default?" row only appears when ``hats.properties`` actually
    specifies a ``hats_cols_default`` list: if a catalog has no default
    columns, that row is left out entirely rather than shown empty.

**Nested-column annotations**
    Same idea for the "Nested?" row: it only shows up if the catalog has at
    least one ``NestedDtype`` column (a column backed by nested Parquet
    structs).

**Collection-specific sections**
    Margin and index sub-catalogs only get listed if ``collection.properties``
    actually records them.  No margins and no indexes means those sections
    just don't appear.

**Association catalogs**
    Association catalogs get their own dedicated template, since they're
    describing a relationship rather than a dataset — it adds a cross-match
    section up front (primary/join catalogs and columns, max separation)
    before the usual column table.  Because of that, you'll never see a
    cone-search example or a "Default?" row for these — see
    `Association catalog additional variables`_.

Custom templates
-------------------------------------------------------------------------------

Want full control over the layout? Pass a Jinja2 template string in as
``jinja2_template`` and it replaces the built-in template entirely:

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

Templates render with
`Jinja2's StrictUndefined <https://jinja.palletsprojects.com/en/stable/api/#jinja2.StrictUndefined>`__,
so referencing a variable that isn't there fails loudly instead of silently
rendering blank.  That's exactly why you'll want to guard optional context
variables with ``{% if variable %}`` (see the reference below).

If you just need the rendered string and don't need it written to disk, call
:func:`hats.io.summary_file.generate_summary` directly instead.

Honestly, the easiest way to learn the context variables is to open up the
built-in templates in ``src/hats/io/templates/`` and see how they're used.

Generating a custom-format file (e.g. VO registry XML)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set ``fmt=None`` and HATS steps out of the way entirely.  It still reads all
the catalog metadata for you, but the output format and filename are
completely yours to define.  Both ``jinja2_template`` and ``filename`` become
required here, since there's no built-in default to fall back on:

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

Whatever you put in ``extra_template_vars`` gets unpacked straight into
``template.render()`` next to the standard HATS context variables (``name``,
``description``, ``cat_props``, etc.), so your template can use either.

Jinja2 context variable reference
-------------------------------------------------------------------------------

What follows is the full list of variables available inside a template.
Some are marked *optional*, meaning they may be ``None`` or absent for some
catalogs, so always guard them with ``{% if variable %}`` before use.

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
    ``MARGIN``, ``INDEX``, ``MAP``, or ``ASSOCIATION``.  See
    `Association catalog additional variables`_ for the fields specific to
    ``ASSOCIATION``.
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

These get added for standalone :class:`~hats.catalog.catalog.Catalog`
summaries (``catalog_type=OBJECT`` or ``SOURCE``):

* ``has_default_columns`` *(bool)* — ``True`` when the catalog declares default
  columns.
* ``cone_code_example`` *(dict or None)* — ``{"ra": float, "dec": float}`` for
  code examples; ``None`` when no sample data is available.
* ``pixel_map_b64`` *(str or None)* — base64-encoded WebP pixel-map image;
  ``None`` without ``matplotlib``.
* ``density_map_b64`` *(str or None)* — base64-encoded WebP density-map image;
  ``None`` without ``matplotlib``.

Association catalog additional variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These are the fields specific to
:class:`~hats.catalog.association_catalog.association_catalog.AssociationCatalog`
summaries (``catalog_type=ASSOCIATION``).  An association catalog exists to
cross-match two other HATS catalogs together.  It's not meant to be read on
its own, and the auto-generated description reflects that directly
(``"This is the association catalog linking {primary} with {join}."``).

* ``cat_props.primary_catalog`` *(str)* — relative path to the primary catalog
  being joined.
* ``cat_props.primary_column`` *(str)* — join-key column name in the primary
  catalog.
* ``cat_props.join_catalog`` *(str)* — relative path to the secondary
  (joined) catalog.
* ``cat_props.join_column`` *(str)* — join-key column name in the joined
  catalog.
* ``cat_props.assn_max_separation`` *(float or None)* — maximum cross-match
  separation in arcseconds, when recorded.

Even though an association catalog is really about the relationship between
two datasets, it's still a HEALPix-partitioned dataset underneath, so
``pixel_map_b64`` (and ``density_map_b64``) can still get computed as long as
``matplotlib`` is installed and the catalog actually has HEALPix pixels.  The
built-in templates only put ``pixel_map_b64`` to use, in a "Sky coverage"
section.  ``cone_code_example``, on the other hand, is always ``None`` here,
because ``ra_column`` and ``dec_column`` were never part of the required
``ASSOCIATION`` schema to begin with.  ``has_default_columns`` follows the
same pattern: it's still computed, just always ``False``, since association
catalogs never declare ``default_columns``, which is why you won't see a
"Default?" row rendered for them either.
