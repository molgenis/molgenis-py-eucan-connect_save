from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from molgenis.eucan_connect.errors import EucanError
from molgenis.eucan_connect.model import Catalogue, RefEntity


@pytest.fixture
def importer_init():
    with patch("molgenis.eucan_connect.eucan.Importer") as importer_mock:
        yield importer_mock


@pytest.fixture
def lifecycle_init():
    with patch("molgenis.eucan_connect.eucan.LifeCycle") as lifecycle_mock:
        yield lifecycle_mock


def test_check_references(eucan):
    source_data = _mock_source_data()
    new_ref_vals = {
        "Urine": "events_biosamples_type",
        "New Biosample": "events_biosamples_type",
        "Survey Data": "events_datasources_type",
        "Phone call": "events_datasources_type",
        "DNA Database": "events_type_administrative_databases",
        "80-81": "population_recruitment_sources",
        "3-4": "population_recruitment_sources",
    }
    expected_ref_print = []
    for value, column in new_ref_vals.items():
        expected_ref_print.append(
            mock.call(
                "A new reference value ("
                + value
                + ") will be added for "
                + column
                + " in the EUCAN-Connect Catalogue"
            )
        )

    eucan._check_reference_data(source_data)

    biosamples = RefEntity("biosamples")
    datasources = RefEntity("data_sources")
    db_types = RefEntity("database_types")
    recr_sources = RefEntity("recruitment_sources")

    assert eucan.printer.print.mock_calls == expected_ref_print
    assert eucan.ref_data.table_by_type[biosamples].rows[0]["id"] == "blood"
    assert eucan.ref_data.table_by_type[biosamples].rows[1]["id"] == "urine"
    assert eucan.ref_data.table_by_type[biosamples].rows[2]["id"] == "new_biosample"
    assert (
        eucan.ref_data.table_by_type[datasources].rows[0]["id"] == "biological_samples"
    )
    assert eucan.ref_data.table_by_type[datasources].rows[1]["id"] == "questionnaires"
    assert eucan.ref_data.table_by_type[datasources].rows[2]["id"] == "survey_data"
    assert eucan.ref_data.table_by_type[datasources].rows[3]["id"] == "phone_call"
    assert eucan.ref_data.table_by_type[db_types].rows[0]["id"] == "health_databases"
    assert eucan.ref_data.table_by_type[db_types].rows[1]["id"] == "dna_database"
    assert (
        eucan.ref_data.table_by_type[recr_sources].rows[0]["id"] == "general_population"
    )
    assert eucan.ref_data.table_by_type[recr_sources].rows[1]["id"] == "80_till_81"
    assert eucan.ref_data.table_by_type[recr_sources].rows[2]["id"] == "3_till_4"


def test_convert_references(eucan):
    source_data = _mock_source_data()
    converted_source_data = _mock_converted_source_data()

    eucan._convert_reference_data(source_data)

    pd.testing.assert_frame_equal(source_data, converted_source_data)


def test_create_catalogue_data(eucan, fake_catalogue_data):
    catalogue = Catalogue("Test", "succeeds", "test_url", "CatalogueType")
    converted_source_data = _mock_converted_source_data()
    check_cat_data = fake_catalogue_data

    catalogue_data = eucan._create_catalogue_data(catalogue, converted_source_data)
    assert catalogue_data == check_cat_data

    # TODO: Wil ik eigenlijk nog checken dat get_uploadable_data vier keer gecalled is
    #  met die en die parameters maar dit werkt niet als ik 'm mock met
    #  with patch.object(eucan, "get_uploadable_data", autospec=True) as
    #  mock_get_uploadable_data:
    #        catalogue_data =
    #        eucan._create_catalogue_data(catalogue, converted_source_data)
    # TODO: maar als ik dat wel doe dan werkt de assert catalogue_data = check_cat_data
    #  niet meer
    # assert mock_get_uploadable_data.mock_calls == [
    #         mock.call(catalogue, converted_source_data, "events"),
    #         mock.call(catalogue, converted_source_data, "persons"),
    #         mock.call(catalogue, converted_source_data, "population"),
    #         mock.call(catalogue, converted_source_data, "study"),
    #         ]
    # TODO: Dit werkt wel, maar kan alleen de laatste checken
    # mock_get_uploadable_data.assert_called_with(catalogue, converted_source_data,
    #                                            "study")


def test_import_catalogues(
    eucan, importer_init, lifecycle_init, session, ref_data, fake_catalogue_data
):
    lc = Catalogue("LC", "succeeds with warnings", "lifecycle_url", "LifeCycle")
    lc_catalogue_data = fake_catalogue_data
    lc_source_data = _mock_source_data()
    lc_converted_source_data = _mock_converted_source_data()
    lifecycle_init.return_value.lifecycle_data.side_effect = [lc_source_data]
    error = EucanError("error")
    importer_init.return_value.importer.side_effect = [[], error]

    eucan._check_reference_data = MagicMock()
    eucan._check_reference_data.return_value = lc_source_data
    eucan._convert_reference_data = MagicMock()
    eucan._convert_reference_data.return_value = lc_converted_source_data
    eucan._create_catalogue_data = MagicMock()
    eucan._create_catalogue_data.return_value = lc_catalogue_data
    eucan._add_new_ref_data = MagicMock()

    report = eucan.import_catalogues([lc])

    assert eucan.printer.print_catalogue_title.mock_calls == [mock.call(lc)]
    assert lifecycle_init.mock_calls == [
        mock.call(session, eucan.printer),
        mock.call().lifecycle_data(lc),
    ]

    eucan._check_reference_data.assert_called_once_with(lc_source_data)
    eucan._convert_reference_data.assert_called_once_with(lc_source_data)
    eucan._create_catalogue_data.assert_called_once_with(lc, lc_converted_source_data)

    # TODO Werkt niet omdat MagicMock's niet gelijk zijn
    # eucan._add_new_ref_data.assert_called_once_with(ref_data, importer_init, report)

    importer_init.assert_called_once_with(session, eucan.printer)

    # TODO werkt ook niet, zegt dat ie niet gecalled wordt
    # importer_init.import_catalogue_data.assert_called_once_with(lc_catalogue_data)

    # TODO dit werkt ook niet, de import_reference_data lijkt niet gecalled te worden
    # assert importer_init.mock_calls == [
    #     mock.call(session, eucan.printer),
    #     mock.call().import_catalogue_data(lc_catalogue_data),
    #     mock.call().import_reference_data(eucan.ref_data),
    # ]
    assert lc not in report.errors

    # assert report.warnings[lc] == [warning]
    eucan.printer.print_summary.assert_called_once_with(report)


def _mock_source_data():
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


def _mock_converted_source_data():
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
