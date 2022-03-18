from unittest import mock

import pandas as pd

from molgenis.eucan_connect.model import RefEntity
from molgenis.eucan_connect.ref_modifier import RefModifier


def test_check_references(fake_source_data, printer, ref_data):
    new_ref_vals = {
        "Urine": "events_biosamples_type",
        "New Biosample": "events_biosamples_type",
        "Survey Data": "events_datasources_type",
        "Phone call": "events_datasources_type",
        "DNA Database": "events_type_administrative_databases",
        "80-81": "population_recruitment_sources",
        "3-4": "population_recruitment_sources",
    }
    expected_ref_print = [mock.call("Check for new reference values")]
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

    RefModifier(
        printer=printer, ref_data=ref_data, source_data=fake_source_data
    )._check_reference_data()

    biosamples = RefEntity("biosamples")
    datasources = RefEntity("data_sources")
    db_types = RefEntity("database_types")
    recr_sources = RefEntity("recruitment_sources")

    assert printer.print.mock_calls == expected_ref_print
    assert ref_data.table_by_type[biosamples].rows[0]["id"] == "blood"
    assert ref_data.table_by_type[biosamples].rows[1]["id"] == "urine"
    assert ref_data.table_by_type[biosamples].rows[2]["id"] == "new_biosample"
    assert ref_data.table_by_type[datasources].rows[0]["id"] == "biological_samples"
    assert ref_data.table_by_type[datasources].rows[1]["id"] == "questionnaires"
    assert ref_data.table_by_type[datasources].rows[2]["id"] == "survey_data"
    assert ref_data.table_by_type[datasources].rows[3]["id"] == "phone_call"
    assert ref_data.table_by_type[db_types].rows[0]["id"] == "health_databases"
    assert ref_data.table_by_type[db_types].rows[1]["id"] == "dna_database"
    assert ref_data.table_by_type[recr_sources].rows[0]["id"] == "general_population"
    assert ref_data.table_by_type[recr_sources].rows[1]["id"] == "80_till_81"
    assert ref_data.table_by_type[recr_sources].rows[2]["id"] == "3_till_4"


def test_convert_references(
    fake_converted_source_data, fake_source_data, printer, ref_data
):
    RefModifier(
        printer=printer, ref_data=ref_data, source_data=fake_source_data
    )._convert_reference_data()

    pd.testing.assert_frame_equal(fake_source_data, fake_converted_source_data)
