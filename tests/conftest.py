"""
    conftest.py

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""

import json
from ast import literal_eval
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pkg_resources
import pytest

from molgenis.eucan_connect.eucan import Eucan
from molgenis.eucan_connect.importer import Importer
from molgenis.eucan_connect.model import (
    Catalogue,
    CatalogueData,
    RefData,
    RefEntity,
    RefTable,
    Table,
    TableMeta,
    TableType,
)


@pytest.fixture
def session() -> MagicMock:
    session = MagicMock()
    session.url = "url"

    def entity_type(table_name):
        return f"{table_name}"

    session.get_meta = MagicMock(side_effect=entity_type)

    def existing_data(
        table_name, batch_size: str = None, attributes: str = None, q: str = None
    ):
        if table_name == "eucan_biosamples":
            return [{"id": "blood"}]
        if table_name == "eucan_data_sources":
            return [{"id": "biological_samples"}, {"id": "questionnaires"}]
        if table_name == "eucan_database_types":
            return [{"id": "health_databases"}]
        if table_name == "eucan_recruitment_sources":
            return [{"id": "general_population"}]
        if table_name == "eucan_persons":
            return [
                {"id": "person_id", "source_catalogue": {"id": "Test"}},
                {"id": "person_deleted_id", "source_catalogue": {"id": "Test"}},
            ]
        if table_name == "eucan_source_catalogues" and q == "id=in=(TC)":
            return [
                {
                    "_href": "/api/v2/eucan_source_catalogues/TC",
                    "id": "TC",
                    "description": "Test Catalogue",
                    "catalogue_url": "https://test.nl",
                    "catalogue_type": "Test",
                }
            ]
        else:
            return [
                {
                    "_href": "/api/v2/eucan_source_catalogues/TC",
                    "id": "TC",
                    "description": "Test Catalogue",
                    "catalogue_url": "https://test.nl",
                    "catalogue_type": "BirthCohorts",
                },
                {
                    "_href": "/api/v2/eucan_source_catalogues/C2",
                    "id": "C2",
                    "description": "Test Catalogue2",
                    "catalogue_url": "https://test2.nl",
                    "catalogue_type": "Mica",
                },
                {
                    "_href": "/api/v2/eucan_source_catalogues/C3",
                    "id": "C3",
                    "description": "Test Catalogue3",
                    "catalogue_url": "https://test3.nl",
                    "catalogue_type": "DNA_catalogue",
                },
            ]

    session.get = MagicMock(side_effect=existing_data)

    return session


@pytest.fixture
def printer() -> MagicMock:
    return MagicMock()


@pytest.fixture
def eucan(session, printer, ref_data) -> Eucan:
    eucan = Eucan(session)
    eucan.printer = printer
    eucan.ref_data = ref_data
    eucan.importer = importer
    return eucan


@pytest.fixture
def importer(session, printer) -> Importer:
    return Importer(session, printer)


@pytest.fixture
def ref_data():
    biosamples = RefTable.of(
        RefEntity.BIOSAMPLES,
        [
            {"id": "blood", "label": "blood"},
        ],
    )

    data_sources = RefTable.of(
        RefEntity.DATASOURCES,
        [
            {"id": "biological_samples", "label": "biological_samples"},
            {"id": "questionnaires", "label": "questionnaires"},
        ],
    )

    database_types = RefTable.of(
        RefEntity.DATABASETYPES,
        [{"id": "health_databases", "label": "health_databases"}],
    )
    recruitment_sources = RefTable.of(
        RefEntity.RECRUITMENTSOURCES,
        [{"id": "general_population", "label": "general_population"}],
    )

    return RefData.from_dict(
        {
            RefEntity.BIOSAMPLES: biosamples,
            RefEntity.DATASOURCES: data_sources,
            RefEntity.DATABASETYPES: database_types,
            RefEntity.RECRUITMENTSOURCES: recruitment_sources,
        },
    )


@pytest.fixture
def lifecycle_data():
    """
    Reads the data (json-format) from lifecycle_data.json to test with.
    """
    file = open(
        pkg_resources.resource_filename("tests.resources", "lifecycle_data.json"), "r"
    )
    lifecycle_data = json.load(file)
    file.close()
    return lifecycle_data


@pytest.fixture
def lifecycle_created_df():
    """
    Reads the data (csv) from lifecycle_created_df.csv to test with.
    """
    file = pkg_resources.resource_filename(
        "tests.resources", "lifecycle_created_df.csv"
    )
    data = pd.read_csv(file, sep=";")
    # Modify list columns from string to list
    list_columns = [
        "study_dataAccessConditions",
        "persons_contributionType",
        "events_areasOfInformation",
        "events_sampleCategories",
        "events_dataCategories",
        "population_ageGroups",
    ]
    for column in list_columns:
        data[column] = data[column].apply(
            lambda x: literal_eval(x) if pd.notna(x) else x
        )

    # Numbers are read in as type float, convert to right string format
    str_columns = ["events_startYear.name", "events_endYear.name"]
    for column in str_columns:
        data[column] = (
            data[column].fillna(-1).astype(int).astype(str).replace("-1", np.nan)
        )

    # Numbers are read in as type float, convert to the right string format
    str_columns = ["events_startMonth.code", "events_endMonth.code"]
    for column in str_columns:
        data[column] = (
            data[column]
            .fillna(-1)
            .astype(int)
            .astype(str)
            .replace("-1", np.nan)
            .apply("{:0>2}".format)
            .replace("nan", np.nan)
        )

    return data


@pytest.fixture
def lifecycle_converted_df():
    """
    Reads the data (csv) from lifecycle_converted_df.csv to test with.
    """
    file = pkg_resources.resource_filename(
        "tests.resources", "lifecycle_converted_df.csv"
    )
    data = pd.read_csv(file, sep=";")
    # Modify list columns from string to np ndarray as this is the result type
    # from _convert_list_values in LifeCycle
    list_columns = [
        "events_type_administrative_databases",
        "events_biosamples_type",
        "events_datasources_type",
        "population_recruitment_sources",
        "study_principle_investigators",
        "study_contacts",
        "study_data_collection_events",
        "study_populations",
    ]
    for column in list_columns:
        data[column] = data[column].apply(
            lambda x: np.array(eval(x), dtype=object) if pd.notna(x) else x
        )

    return data


@pytest.fixture
def fake_catalogue_data(eucan):
    catalogue = Catalogue("Test", "succeeds", "test_url", "CatalogueType")
    persons_meta = eucan.session.get_meta("eucan_persons")
    persons = Table.of(
        TableType.PERSONS,
        persons_meta,
        [
            {
                "first_name": "Piet",
                "email": "email",
                "id": "person_id",
                "last_name": "Geluk",
                "source_catalogue": "Test",
            },
        ],
    )

    events_meta = eucan.session.get_meta("eucan_events")
    events = Table.of(
        TableType.EVENTS,
        events_meta,
        [
            {
                "name": "events_name",
                "description": "events_description",
                "id": "events_id",
                "start_end_year": "2021-2022",
                "biosamples_type": ["blood", "urine", "new_biosample"],
                "datasources_type": ["survey_data", "phone_call"],
                "type_administrative_databases": ["health_databases", "dna_database"],
                "source_catalogue": "Test",
            },
        ],
    )

    populations_meta = eucan.session.get_meta("eucan_population")
    populations = Table.of(
        TableType.POPULATIONS,
        populations_meta,
        [
            {
                "name": "pop_name",
                "number_of_participants": 30,
                "selection_criteria_supplement": "pop_sup",
                "description": "pop_desc",
                "id": "pop_id",
                "recruitment_sources": ["80_till_81", "3_till_4"],
                "source_catalogue": "Test",
            },
        ],
    )

    study_meta = eucan.session.get_meta("eucan_study")
    studies = Table.of(
        TableType.STUDIES,
        study_meta,
        [
            {
                "id": "id",
                "study_name": "name",
                "acronym": "acronym",
                "objectives": "objectives",
                "start_year": 2021,
                "end_year": 2022,
                "website": "website",
                "funding": "funding",
                "number_of_participants": 200,
                "dataAccessConditions": "accessConditions",
                "contact_procedures": "contact_procedures",
                "study_design": "study_design",
                "marker_paper": "marker_paper",
                "participants_with_biosamples": 150,
                "access_possibility": "access_poss",
                "principle_investigators": ["person1", "person2"],
                "contacts": ["person3", "person4"],
                "data_collection_events": ["events_id"],
                "populations": ["pop_id"],
                "source_catalogue": "Test",
            },
        ],
    )

    return CatalogueData.from_dict(
        catalogue=catalogue,
        source=catalogue.description,
        tables={
            TableType.PERSONS: persons,
            TableType.EVENTS: events,
            TableType.POPULATIONS: populations,
            TableType.STUDIES: studies,
        },
    )


@pytest.fixture
def fake_source_data():
    df = pd.DataFrame(
        {
            "study_id": ["id"],
            "study_study_name": ["name"],
            "study_acronym": ["acronym"],
            "study_objectives": ["objectives"],
            "study_start_year": [2021],
            "study_end_year": [2022],
            "study_website": ["website"],
            "study_funding": ["funding"],
            "study_number_of_participants": [200],
            "study_dataAccessConditions": ["accessConditions"],
            "study_contact_procedures": ["contact_procedures"],
            "study_study_design": ["study_design"],
            "study_marker_paper": ["marker_paper"],
            "study_participants_with_biosamples": [150],
            "persons_first_name": ["Piet"],
            "persons_email": ["email"],
            "events_name": ["events_name"],
            "events_description": ["events_description"],
            "population_name": ["pop_name"],
            "population_number_of_participants": [30],
            "population_selection_criteria_supplement": ["pop_sup"],
            "population_description": ["pop_desc"],
            "persons_id": ["person_id"],
            "events_id": ["events_id"],
            "population_id": ["pop_id"],
            "persons_last_name": ["Geluk"],
            "events_start_end_year": ["2021-2022"],
            "study_access_possibility": ["access_poss"],
            "events_biosamples_type": [["blood", "Urine", "New Biosample"]],
            "events_datasources_type": [["Survey Data", "Phone call"]],
            "events_type_administrative_databases": [
                ["health_databases", "DNA Database"]
            ],
            "population_recruitment_sources": [["80-81", "3-4"]],
            "study_principle_investigators": [["person1", "person2"]],
            "study_contacts": [["person3", "person4"]],
            "study_data_collection_events": [["events_id"]],
            "study_populations": [["pop_id"]],
        }
    )
    return df


@pytest.fixture()
def fake_converted_source_data():
    df = pd.DataFrame(
        {
            "study_id": ["id"],
            "study_study_name": ["name"],
            "study_acronym": ["acronym"],
            "study_objectives": ["objectives"],
            "study_start_year": [2021],
            "study_end_year": [2022],
            "study_website": ["website"],
            "study_funding": ["funding"],
            "study_number_of_participants": [200],
            "study_dataAccessConditions": ["accessConditions"],
            "study_contact_procedures": ["contact_procedures"],
            "study_study_design": ["study_design"],
            "study_marker_paper": ["marker_paper"],
            "study_participants_with_biosamples": [150],
            "persons_first_name": ["Piet"],
            "persons_email": ["email"],
            "events_name": ["events_name"],
            "events_description": ["events_description"],
            "population_name": ["pop_name"],
            "population_number_of_participants": [30],
            "population_selection_criteria_supplement": ["pop_sup"],
            "population_description": ["pop_desc"],
            "persons_id": ["person_id"],
            "events_id": ["events_id"],
            "population_id": ["pop_id"],
            "persons_last_name": ["Geluk"],
            "events_start_end_year": ["2021-2022"],
            "study_access_possibility": ["access_poss"],
            "events_biosamples_type": [["blood", "urine", "new_biosample"]],
            "events_datasources_type": [["survey_data", "phone_call"]],
            "events_type_administrative_databases": [
                ["health_databases", "dna_database"]
            ],
            "population_recruitment_sources": [["80_till_81", "3_till_4"]],
            "study_principle_investigators": [["person1", "person2"]],
            "study_contacts": [["person3", "person4"]],
            "study_data_collection_events": [["events_id"]],
            "study_populations": [["pop_id"]],
        }
    )
    return df


@pytest.fixture()
def meta_data():
    meta = TableMeta(
        meta={
            "links": {"self": "https://test_url"},
            "data": {
                "id": "eucan_something",
                "label": "Test",
                "attributes": {
                    "items": [
                        {
                            "data": {
                                "id": "aaaac",
                                "label": "ID",
                                "name": "id",
                                "type": "string",
                                "idAttribute": True,
                            }
                        }
                    ]
                },
            },
        }
    )
    return meta
