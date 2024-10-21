from __future__ import annotations

import os
import warnings
from typing import List

import pandas as pd

from hats.catalog.catalog_type import CatalogType
from hats.catalog.dataset.dataset import Dataset
from hats.inspection.almanac_info import AlmanacInfo
from hats.io.file_io import file_pointer
from hats.loaders import read_hats


class Almanac:
    """Single instance of an almanac, and available catalogs within namespaces

    Params:
        include_default_dir:
            include directory indicated in HATS_ALMANAC_DIR
            environment variable. see AlmanacInfo.get_default_dir
        dirs:
            additional directories or files to look for almanac files in. we support a
            few types of input, with different behaviors:

            - ``str`` - a single directory (or file)
            - ``list[str]`` - multiple directories (or files)
            - ``dict[str:str]`` / ``dict[str:list[str]]`` - namespace
              dictionary. for each key in the dictionary, we put all almanac
              entries under a namespace. this is useful if you have name
              collisions e.g. between multiple surveys or user-provided
              catalogs.
    """

    def __init__(self, include_default_dir=True, dirs=None):
        """Create new almanac."""
        self.files = {}
        self.entries = {}
        self.dir_to_catalog_name = {}
        self._init_files(include_default_dir=include_default_dir, dirs=dirs)
        self._init_catalog_objects()
        self._init_catalog_links()

    def _init_files(self, include_default_dir=True, dirs=None):
        """Create a list of all the almanac files we want to add to this instance.

        Each almanac file corresponds to a single catalog.

        Args:
            include_default_dir: include directory indicated in HATS_ALMANAC_DIR
                environment variable. see AlmanacInfo.get_default_dir
            dirs: additional directories to look for almanac files in
        """
        if include_default_dir:
            default_dir = AlmanacInfo.get_default_dir()
            if default_dir:
                self._add_files_to_namespace(default_dir)
        if pd.api.types.is_dict_like(dirs):
            for key, value in dirs.items():
                self._add_files_to_namespace(value, key)
        elif pd.api.types.is_list_like(dirs):
            self._add_files_to_namespace(dirs)
        elif dirs is not None:
            self._add_files_to_namespace(dirs)

    def _add_files_to_namespace(self, directory, namespace=""):
        """Get almanac files within a directory or list of directories.

        Args:
            directory: directory to scan
            namespace: if provided, files in this directory will be in their
                own namespace in the almanac
        """
        if not pd.api.types.is_list_like(directory):
            directory = [directory]

        files = []
        for input_path in directory:
            if os.path.isfile(input_path):
                files.append(input_path)
                continue

            path_contents = file_pointer.get_directory_contents(input_path)
            input_paths = [x for x in path_contents if str(x).endswith(".yml")]
            input_paths.sort()
            files.extend(input_paths)

        if namespace in self.files:
            self.files[namespace].extend(files)
        else:
            self.files[namespace] = files

    def _init_catalog_objects(self):
        """Create (unlinked) almanac info objects for all the files found
        in the previous steps."""
        for namespace, files in self.files.items():
            for file in files:
                catalog_info = AlmanacInfo.from_file(file)
                catalog_info.namespace = namespace
                if namespace:
                    full_name = f"{namespace}:{catalog_info.catalog_name}"
                else:
                    full_name = catalog_info.catalog_name
                if full_name in self.entries:
                    warnings.warn(f"Duplicate catalog name ({full_name}). Try using namespaces.")
                else:
                    self.entries[full_name] = catalog_info
                    self.dir_to_catalog_name[catalog_info.catalog_path] = full_name

    def _init_catalog_links(self):
        """Initialize the links between almanac catalogs.

        For each type of link (e.g. primary or join), look for the catalog in
        the almanac, using whatever text we have. If found, add the object
        to the almanac info as a pointer. Additionally, add the reference to
        the linked object, so catalogs know about each other from either side.
        """
        for catalog_entry in self.entries.values():
            if catalog_entry.catalog_type == CatalogType.OBJECT:
                ## Object currently has no links that start in the catalog.
                pass
            elif catalog_entry.catalog_type == CatalogType.SOURCE:
                ## Source catalogs MAY indicate their primary object catalog.
                if catalog_entry.primary:
                    object_catalog = self._get_linked_catalog(catalog_entry.primary, catalog_entry.namespace)
                    if not object_catalog:
                        warnings.warn(
                            f"source catalog {catalog_entry.catalog_name} missing "
                            f"object catalog {catalog_entry.primary}"
                        )
                    else:
                        catalog_entry.primary_link = object_catalog
                        catalog_entry.objects.append(object_catalog)
                        object_catalog.sources.append(catalog_entry)
            elif catalog_entry.catalog_type == CatalogType.ASSOCIATION:
                ## Association table MUST have a primary and join catalog
                primary_catalog = self._get_linked_catalog(catalog_entry.primary, catalog_entry.namespace)
                if not primary_catalog:
                    warnings.warn(
                        f"association table {catalog_entry.catalog_name} missing "
                        f"primary catalog {catalog_entry.primary}"
                    )
                else:
                    catalog_entry.primary_link = primary_catalog
                    primary_catalog.associations.append(catalog_entry)

                join_catalog = self._get_linked_catalog(
                    catalog_entry.join,
                    catalog_entry.namespace,
                )
                if not join_catalog:
                    warnings.warn(
                        f"association table {catalog_entry.catalog_name} missing "
                        f"join catalog {catalog_entry.join}"
                    )
                else:
                    catalog_entry.join_link = join_catalog
                    join_catalog.associations_right.append(catalog_entry)
            elif catalog_entry.catalog_type == CatalogType.MARGIN:
                ## Margin catalogs MUST have a primary catalog
                primary_catalog = self._get_linked_catalog(catalog_entry.primary, catalog_entry.namespace)
                if not primary_catalog:
                    warnings.warn(
                        f"margin table {catalog_entry.catalog_name} missing "
                        f"primary catalog {catalog_entry.primary}"
                    )
                else:
                    catalog_entry.primary_link = primary_catalog
                    primary_catalog.margins.append(catalog_entry)
            elif catalog_entry.catalog_type == CatalogType.INDEX:
                ## Index tables MUST have a primary catalog
                primary_catalog = self._get_linked_catalog(catalog_entry.primary, catalog_entry.namespace)
                if not primary_catalog:
                    warnings.warn(
                        f"index table {catalog_entry.catalog_name} missing "
                        f"primary catalog {catalog_entry.primary}"
                    )
                else:
                    catalog_entry.primary_link = primary_catalog
                    primary_catalog.indexes.append(catalog_entry)
            else:  # pragma: no cover
                warnings.warn(f"Unknown catalog type {catalog_entry.catalog_type}")

    def _get_linked_catalog(self, linked_text, namespace) -> AlmanacInfo | None:
        """Find a catalog to be used for linking catalogs within the almanac.

        e.g. for an association table, we will have a primary and join catalog.
        the association catalog is "receiving" the link of primary catalog info,
        and a link of join catalog info.

        Args:
            linked_text: text provided for the linked catalog. this could take
                a few different forms:

                - empty or None (returns None)
                - short name of a catalog
                - namespaced name of a catalog
                - full path to a catalog base directory
                - path to a catalog base directory, with environment variables
            namespace: the namespace in the catalog **receiving** the link.
                this is used to resolve the linked_text argument, so if you're
                relying on namespaces, the receiving and linking catalog should
                be in the same namespace
        Returns:
            almanac info for the linked catalog, if found
        """
        resolved_path = os.path.expandvars(linked_text)
        if linked_text in self.dir_to_catalog_name:  # pragma: no cover
            linked_text = self.dir_to_catalog_name[linked_text]
        elif resolved_path in self.dir_to_catalog_name:
            linked_text = self.dir_to_catalog_name[resolved_path]

        resolved_name = linked_text
        if resolved_name not in self.entries:
            resolved_name = f"{namespace}:{linked_text}"
            if resolved_name not in self.entries:
                return None
        return self.entries[resolved_name]

    def catalogs(self, include_deprecated=False, types: List[str] | None = None):
        """Get names of catalogs in the almanac, matching the provided conditions.

        Catalogs must meet all criteria provided in order to be returned (e.g.
        the criteria are ANDED together).

        Args:
            include_deprecated: include catalogs which contain some text in their
                ``deprecated`` field.
            types: include ONLY catalogs within the list of provided types.
        """
        selected = []
        for full_name, catalog_info in self.entries.items():
            include = True
            if not include_deprecated and catalog_info.deprecated:
                include = False
            if types and catalog_info.catalog_type not in types:
                include = False

            if include:
                selected.append(full_name)
        return selected

    def get_almanac_info(self, catalog_name: str) -> AlmanacInfo:
        """Fetch the almanac info for a single catalog."""
        return self.entries[catalog_name]

    def get_catalog(self, catalog_name: str) -> Dataset:
        """Fetch the fully-populated hats metadata for the catalog name.

        This will load the ``catalog_info.join`` and other relevant metadata files
        from disk."""
        return read_hats(self.get_almanac_info(catalog_name=catalog_name).catalog_path)
