
HATS v0.6.2  (2025-07-26)
==========================================

Bugfixes
-------------------------

- prevent jproperties from escaping colons in values (`#523 <https://github.com/astronomy-commons/hats/pull/523>`__)
- Better type-handling in parquet statistics (`#532 <https://github.com/astronomy-commons/hats/pull/532>`__)
- Add block sizes to UPath (`#536 <https://github.com/astronomy-commons/hats/pull/536>`__)
- Support margin filtering with larger Margin thresholds (`#535 <https://github.com/astronomy-commons/hats/pull/535>`__)

Improved Documentation
-------------------------

- Update getting_started.rst (`#530 <https://github.com/astronomy-commons/hats/pull/530>`__)

HATS v0.6.1  (2025-07-16)
==========================================

Bugfixes
-------------------------

- Do not swallow file system errors when reading catalogs. (`#518 <https://github.com/astronomy-commons/hats/pull/518>`__)
- Check if catalogs are complete on validation (`#521 <https://github.com/astronomy-commons/hats/pull/521>`__)
- Check if pixel file exists needs filesystem (`#522 <https://github.com/astronomy-commons/hats/pull/522>`__)

Improved Documentation
-------------------------

- Update testing-windows.yml (`#517 <https://github.com/astronomy-commons/hats/pull/517>`__)
- raise error when there are no remaining pixels to merge or plot (`#526 <https://github.com/astronomy-commons/hats/pull/526>`__)
- raise error when plot_pixels is called on an empty region (`#525 <https://github.com/astronomy-commons/hats/pull/525>`__)
- Update directory scheme documentation (`#527 <https://github.com/astronomy-commons/hats/pull/527>`__)
- Generate provenance properties (`#529 <https://github.com/astronomy-commons/hats/pull/529>`__)

New Contributors
-------------------------

* @jaladh-singhal made their first contribution in https://github.com/astronomy-commons/hats/pull/527


HATS v0.6  (2025-06-11)
==========================================

Misc
-------------------------

- rename plot pixels function header (`#514 <https://github.com/astronomy-commons/hats/pull/514>`__)
- Allow alternate skymaps (and unknown properties) (`#511 <https://github.com/astronomy-commons/hats/pull/511>`__)
- Skip pyarrow version 19.0.0 (`#516 <https://github.com/astronomy-commons/hats/pull/516>`__)

New Contributors
-------------------------
- @kesiavino made their first contribution in https://github.com/astronomy-commons/hats/pull/514

HATS v0.5.3  (2025-06-02)
==========================================

Features
-------------------------

- Generate data thumbnail (`#503 <https://github.com/astronomy-commons/hats/pull/503>`__)
- Change default for `create_thumbnail` flag (`#504 <https://github.com/astronomy-commons/hats/pull/504>`__)
- Write additional hats.properties file. (`#512 <https://github.com/astronomy-commons/hats/pull/512>`__)

Misc
-------------------------
- Patch colorbar label in `plot_density()` (`#506 <https://github.com/astronomy-commons/hats/pull/506>`__)
- Add more imports to inits. (`#507 <https://github.com/astronomy-commons/hats/pull/507>`__)


HATS v0.5.2  (2025-05-07)
==========================================

Features
-------------------------

- Catalog collection (`#490 <https://github.com/astronomy-commons/hats/pull/490>`__)
- Add original schema property to dataset (`#492 <https://github.com/astronomy-commons/hats/pull/492>`__)

Bugfixes
-------------------------
- Update moc fov to determine order based on FOV size (`#491 <https://github.com/astronomy-commons/hats/pull/491>`__)
- Change empty catalog max order behaviour (`#495 <https://github.com/astronomy-commons/hats/pull/495>`__)
- add int64 dtype conversion to pixel tree (`#494 <https://github.com/astronomy-commons/hats/pull/494>`__)
- Handle list and dict input for properties creation. (`#496 <https://github.com/astronomy-commons/hats/pull/496>`__)

Misc
-------------------------
- Update to PPT v2.0.6 (`#493 <https://github.com/astronomy-commons/hats/pull/493>`__)
- Update `read_parquet_file_to_pandas` to use nested pandas I/O (`#499 <https://github.com/astronomy-commons/hats/pull/499>`__)
- Update PPT and add lowest supported versions (`#502 <https://github.com/astronomy-commons/hats/pull/502>`__)

Improved Documentation
-------------------------

- Add github button to docs (`#498 <https://github.com/astronomy-commons/hats/pull/498>`__)

HATS v0.5.1  (2025-04-17)
==========================================

Features
-------------------------

- Granular, per-pixel statistics method. (`#477 <https://github.com/astronomy-commons/hats/pull/477>`__)

Bugfixes
-------------------------
- Rename ``argument -> multi_index`` (`#479 <https://github.com/astronomy-commons/hats/pull/479>`__)

Deprecations and Removals
-------------------------
- Remove almanac (`#488 <https://github.com/astronomy-commons/hats/pull/488>`__)

Misc
-------------------------
- Better type hints for new methods/args. (`#478 <https://github.com/astronomy-commons/hats/pull/478>`__)
- Add ``extra_dict`` method to ``TableProperties`` (`#484 <https://github.com/astronomy-commons/hats/pull/484>`__)
- Allow single input for compute_spatial_index (`#486 <https://github.com/astronomy-commons/hats/pull/486>`__)
- Pydantic class for collection properties (`#485 <https://github.com/astronomy-commons/hats/pull/485>`__)
- Unpin matplotlib (`#489 <https://github.com/astronomy-commons/hats/pull/489>`__)


HATS v0.5.0  (2025-03-19)
==========================================

Features
-------------------------
- Enable anonymous S3 access by default (`#466 <https://github.com/astronomy-commons/hats/pull/466>`__)
- Support Npix partitions with a different file suffix or that are directories (`#458 <https://github.com/astronomy-commons/hats/pull/458>`__)
- Always provide partitioning=None and filesystem (`#469 <https://github.com/astronomy-commons/hats/pull/469>`__)
- Filtered catalog should retain path. Add friendlier check for in-memo… (`#470 <https://github.com/astronomy-commons/hats/pull/470>`__)
- Expand column statistics to limit by pixels (`#472 <https://github.com/astronomy-commons/hats/pull/472>`__)
- Move collection of hive column names to shared library. (`#475 <https://github.com/astronomy-commons/hats/pull/475>`__)

Deprecations and Removals
-------------------------
- Remove utilities to write pixel-only data to parquet metadata files. (`#471 <https://github.com/astronomy-commons/hats/pull/471>`__)
- Remove reading partition info pixels from Norder/Npix (`#474 <https://github.com/astronomy-commons/hats/pull/474>`__)

Misc
-------------------------
- Re-generate test data, and update expectations. (`#467 <https://github.com/astronomy-commons/hats/pull/467>`__)


HATS v0.4.7  (2025-03-04)
==========================================

Bugfixes
-------------------------
- Suppress NaN warnings with context manager. (`#453 <https://github.com/astronomy-commons/hats/pull/453>`__)
- Be safer around none values in metadata statistics. (`#460 <https://github.com/astronomy-commons/hats/pull/460>`__)
- Don't pass additional kwargs to file open. (`#465 <https://github.com/astronomy-commons/hats/pull/465>`__)

Improved Documentation
-------------------------

- Change non-anchoring links to "anonymous" links. (`#454 <https://github.com/astronomy-commons/hats/pull/454>`__)
- Add example for anonymous S3 catalog reads (`#459 <https://github.com/astronomy-commons/hats/pull/459>`__)


HATS v0.4.6  (2025-01-23)
==========================================

Features
-------------------------
- Use a naive sparse histogram. (`#446 <https://github.com/astronomy-commons/hats/pull/446>`__)
- Add testing for python 3.13 (`#449 <https://github.com/astronomy-commons/hats/pull/449>`__)

Bugfixes
-------------------------
- Ensure use of float64 when calling radec2pix (`#447 <https://github.com/astronomy-commons/hats/pull/447>`__)

Misc
-------------------------
- Remove typing imports for List, Tuple, Union (`#441 <https://github.com/astronomy-commons/hats/pull/441>`__)
- Update to PPT 2.0.5 - fixes slack notifications (`#443 <https://github.com/astronomy-commons/hats/pull/443>`__)

Improved Documentation
-------------------------

- Documentation improvements. (`#445 <https://github.com/astronomy-commons/hats/pull/445>`__)

New Contributors
-------------------------
- @gitosaurus made their first contribution in https://github.com/astronomy-commons/hats/pull/447


HATS v0.4.5  (2024-12-06)
==========================================

Features
-------------------------
- Make point_map.fits plotting more friendly. (`#439 <https://github.com/astronomy-commons/hats/pull/439>`__)

Misc
-------------------------
- Update PPT to 2.0.4 (`#438 <https://github.com/astronomy-commons/hats/pull/438>`__)
- Try windows test workflow (`#440 <https://github.com/astronomy-commons/hats/pull/440>`__)
- Move window of supported python versions. (`#442 <https://github.com/astronomy-commons/hats/pull/442>`__)

HATS v0.4.4  (2024-11-26)
==========================================

Features
-------------------------
- Add version property (`#418 <https://github.com/astronomy-commons/hats/pull/418>`__)
- Create new catalog type: map (`#429 <https://github.com/astronomy-commons/hats/pull/429>`__)

Misc
-------------------------
- Migrate polygon search to use mocpy utilities (`#415 <https://github.com/astronomy-commons/hats/pull/415>`__)
- Capture compression and open binary if present. (`#419 <https://github.com/astronomy-commons/hats/pull/419>`__)
- Vectorize polygon validation (`#431 <https://github.com/astronomy-commons/hats/pull/431>`__)
- Remove margin fine filtering, and healpy dependency. (`#434 <https://github.com/astronomy-commons/hats/pull/434>`__)


HATS v0.4.3  (2024-11-07)
==========================================

Features
-------------------------
- Improve catalog validation and column statistics (`#404 <https://github.com/astronomy-commons/hats/pull/404>`__)
- Write point map with cdshealpix skymap (`#409 <https://github.com/astronomy-commons/hats/pull/409>`__)
- add moc plotting method (`#414 <https://github.com/astronomy-commons/hats/pull/414>`__)

Bugfixes
-------------------------
- Correct pixel boundaries when plotting pixels at orders lower than 3 show (`#413 <https://github.com/astronomy-commons/hats/pull/413>`__)

Improved Documentation
-------------------------

- Update cone search notebook (`#405 <https://github.com/astronomy-commons/hats/pull/405>`__)

HATS v0.4.2  (2024-10-29)
==========================================

Features
-------------------------
- Introduce aggregate_column_statistics (`#387 <https://github.com/astronomy-commons/hats/pull/387>`__)
- Add custom healpix plotting method (`#374 <https://github.com/astronomy-commons/hats/pull/374>`__)
- Allow custom plot title (`#396 <https://github.com/astronomy-commons/hats/pull/396>`__)

Bugfixes
-------------------------
- Simplify catalog reading (`#394 <https://github.com/astronomy-commons/hats/pull/394>`__)
- Minor plotting fixes (`#403 <https://github.com/astronomy-commons/hats/pull/403>`__)

Misc
-------------------------
- Unpin astropy version (`#391 <https://github.com/astronomy-commons/hats/pull/391>`__)
- Update copier (`#388 <https://github.com/astronomy-commons/hats/pull/388>`__)
- Merge development branch (`#389 <https://github.com/astronomy-commons/hats/pull/389>`__)
- Convenience method to estimate mindist for a given order. (`#392 <https://github.com/astronomy-commons/hats/pull/392>`__)
- Use numba jit compilation instead of precompilation (`#395 <https://github.com/astronomy-commons/hats/pull/395>`__)


HATS v0.4.1  (2024-10-17)
==========================================

Misc
-------------------------
- Fix broken unittests (`#383 <https://github.com/astronomy-commons/hats/pull/383>`__)
- Pin astropy temporarily (`#384 <https://github.com/astronomy-commons/hats/pull/384>`__)

Improved Documentation
-------------------------

- Documentation sweep (`#381 <https://github.com/astronomy-commons/hats/pull/381>`__)
- Add a getting started page (`#382 <https://github.com/astronomy-commons/hats/pull/382>`__)

New Contributors
-------------------------
* @jeremykubica made their first contribution in https://github.com/astronomy-commons/hats/pull/383


HATS v0.4.0  (2024-10-16)
==========================================

This is the first release under the name ``HATS``. This package was previously
known as ``hipscat``.

Features
-------------------------
- Hats renaming (`#379 <https://github.com/astronomy-commons/hats/pull/379>`__)
- Replace FilePointer with universal pathlib (`#336 <https://github.com/astronomy-commons/hats/pull/336>`__)

Deprecations and Removals
-------------------------
- Replace FilePointer with universal pathlib (`#336 <https://github.com/astronomy-commons/hats/pull/336>`__)
- Limit kwargs passed to file.open. (`#341 <https://github.com/astronomy-commons/hats/pull/341>`__)
- Remove unused methods in pixel margin module (`#343 <https://github.com/astronomy-commons/hats/pull/343>`__)

Misc
-------------------------
- Provide better type hints for path-like arguments. (`#342 <https://github.com/astronomy-commons/hats/pull/342>`__)
