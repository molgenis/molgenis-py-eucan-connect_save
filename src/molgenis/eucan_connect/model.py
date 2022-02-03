import typing
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


class TableType(Enum):
    """Enum representing the tables the core EUCAN-Connect Catalogue has."""

    EVENTS = "events"
    PERSONS = "persons"
    POPULATIONS = "population"
    STUDIES = "study"

    @classmethod
    def get_import_order(cls) -> List["TableType"]:
        return [type_ for type_ in cls]

    @property
    def table(self) -> str:
        return f"{self.value}"

    @property
    def base_id(self) -> str:
        return f"eucan_{self.value}"


@dataclass(frozen=True)
class TableMeta:
    """Convenient wrapper for the output of the metadata API."""

    meta: dict

    @property
    def id_attribute(self):
        for attribute in self.meta["data"]["attributes"]["items"]:
            if attribute["data"]["idAttribute"] is True:
                return attribute["data"]["name"]


@dataclass(frozen=True)
class Table:
    """
    Simple representation of a EUCAN-Connect table. The rows should be in the
    uploadable format. (See _utils.py)
    """

    type: TableType
    rows_by_id: "typing.OrderedDict[str, dict]"
    meta: TableMeta

    @property
    def rows(self) -> List[dict]:
        return list(self.rows_by_id.values())

    @staticmethod
    def of(table_type: TableType, meta: TableMeta, rows: List[dict]) -> "Table":
        """Factory method that takes a list of rows instead of an OrderedDict of
        ids/rows."""
        rows_by_id = OrderedDict()
        for row in rows:
            rows_by_id[row["id"]] = row
        return Table(
            type=table_type,
            meta=meta,
            rows_by_id=rows_by_id,
        )


@dataclass(frozen=True)
class Catalogue:
    """Represents a single source catalogue in the EUCAN-Connect catalogue."""

    code: str
    description: str
    catalogue_url: str
    catalogue_type: str

    _classifiers = {
        TableType.PERSONS: "contactID",
        TableType.EVENTS: "eventID",
        TableType.POPULATIONS: "populationID",
        TableType.STUDIES: "studyID",
    }

    def get_id_prefix(self, table_type: TableType) -> str:
        """
        Each table has a specific prefix for the identifiers of its rows. This prefix is
        based on the source catalogue's description and the classifier of the table.

        :param TableType table_type: the table to get the id prefix for
        :return: the id prefix
        """
        classifier = self._classifiers[table_type]
        source = self.description.lower().replace(" ", "")
        return f"{source}:{classifier}:"


@dataclass()
class CatalogueData:
    """Container object storing the four tables of a single node."""

    catalogue: Catalogue
    source: str
    persons: Table
    events: Table
    populations: Table
    studies: Table
    table_by_type: Dict[TableType, Table]

    @property
    def import_order(self) -> List[Table]:
        return [self.persons, self.events, self.populations, self.studies]

    @staticmethod
    def from_dict(
        catalogue: Catalogue, source: str, tables: Dict[TableType, Table]
    ) -> "CatalogueData":
        return CatalogueData(
            catalogue=catalogue,
            source=source,
            persons=tables[TableType.PERSONS],
            events=tables[TableType.EVENTS],
            populations=tables[TableType.POPULATIONS],
            studies=tables[TableType.STUDIES],
            table_by_type=tables,
        )


@dataclass(frozen=True)
class IsoCountryData:
    """
    Stores all available ISO (two and three letter) country codes
    and the country names available in the EUCAN-Connect Catalogue
    """

    iso_country_data: List[dict]
    """List with a dictionary per country"""

    def get_country_id(self, country_description: str) -> str:
        iso_country = None
        for iso_type in self.iso_country_data[0].keys():
            iso_country = next(
                (
                    country
                    for country in self.iso_country_data
                    if country[iso_type] == country_description
                ),
                False,
            )
            if iso_country:
                break
        if iso_country:
            return iso_country["iso2_code"]
        else:
            return ""


class RefEntity(Enum):
    """Enum representing the reference entities the EUCAN-Connect Catalogue has."""

    BIOSAMPLES = "biosamples"
    DATASOURCES = "data_sources"
    DATABASETYPES = "database_types"
    RECRUITMENTSOURCES = "recruitment_sources"

    @classmethod
    def get_ref_entities(cls) -> List["RefEntity"]:
        return [type_ for type_ in cls]

    @property
    def base_id(self) -> str:
        return f"eucan_{self.value}"


@dataclass(frozen=True)
class RefTable:
    """
    Simple representation of a EUCAN-Connect reference table.
    """

    type: RefEntity
    rows_by_id: "typing.OrderedDict[str, dict]"

    @property
    def rows(self) -> List[dict]:
        return list(self.rows_by_id.values())

    @staticmethod
    def of(table_type: RefEntity, rows: List[dict]) -> "RefTable":
        """Factory method that takes a list of rows instead of an OrderedDict of
        ids/rows."""
        rows_by_id = OrderedDict()
        for row in rows:
            rows_by_id[row["id"]] = row
        return RefTable(
            type=table_type,
            rows_by_id=rows_by_id,
        )


@dataclass()
class RefData:
    """Container object storing the reference entity data."""

    # ref_entity: RefEntity
    table_by_type: Dict[RefEntity, RefTable]

    @staticmethod
    def invalid_id_characters():
        replacements = [
            {" ": "_"},
            {"-": "_till_"},
            {"/": "_or_"},
            {"+": "Plus"},
            {"<": "before_"},
        ]

        return replacements

    def add_new_ref(self, ref_entity, new_ref, ref_description):
        refs = self.table_by_type[RefEntity(ref_entity)].rows
        refs.append({"id": new_ref, "label": ref_description})
        self.table_by_type[RefEntity(ref_entity)] = RefTable.of(
            table_type=ref_entity, rows=refs
        )

    #  @property TODO: is dit een property?
    def all_refs(self, ref_entity) -> List[str]:
        all_refs = list(self.table_by_type[RefEntity(ref_entity)].rows_by_id.keys())
        return all_refs

    @staticmethod
    def from_dict(tables: Dict[RefEntity, RefTable]) -> "RefData":
        return RefData(
            # ref_entity=ref_entity,
            table_by_type=tables,
        )
