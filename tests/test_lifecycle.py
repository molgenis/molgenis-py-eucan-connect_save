from unittest import mock
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from molgenis.eucan_connect.lifecycle import LifeCycle
from molgenis.eucan_connect.model import Catalogue


def test_lifecycle_data(
    eucan, lifecycle_data, lifecycle_created_df, lifecycle_converted_df
):
    catalogue = Catalogue("LC", "LifeCycle", "lifecycle_url", "LifeCycle")
    lifecycle = LifeCycle(eucan, eucan.printer, catalogue)
    lifecycle.get_lc_cohort_data = MagicMock()
    lifecycle.get_lc_cohort_data.return_value = lifecycle_data
    lifecycle._create_df = MagicMock()
    lifecycle._create_df.return_value = lifecycle_created_df
    lifecycle._convert_values = MagicMock()
    lifecycle._convert_values.return_value = lifecycle_created_df

    lifecycle.lifecycle_data()
    lifecycle.get_lc_cohort_data.assert_called_once()

    assert eucan.printer.print.mock_calls == [mock.call("🗑 Get LifeCycle studies")]
    assert eucan.printer.print_sub_header.mock_calls == [
        mock.call("Number of cohorts retrieved for LifeCycle is 2")
    ]

    lifecycle._create_df.assert_called_once_with(lifecycle_data)
    lifecycle._convert_values.assert_called_once_with(lifecycle_created_df)


def test_create_df_and_convert_values(
    eucan, lifecycle_data, lifecycle_created_df, lifecycle_converted_df
):
    catalogue = Catalogue("LC", "LifeCycle", "lifecycle_url", "LifeCycle")
    lifecycle = LifeCycle(eucan, eucan.printer, catalogue)
    lifecycle._convert_list_values = MagicMock(
        side_effect=lifecycle._convert_list_values
    )
    lifecycle._extract_data = MagicMock(side_effect=lifecycle._extract_data)
    lifecycle._group_column_information = MagicMock(
        side_effect=lifecycle._group_column_information
    )

    created_df = lifecycle._create_df(lifecycle_data)

    pd.testing.assert_frame_equal(created_df, lifecycle_created_df)

    converted_df = lifecycle._convert_values(created_df)

    # Remove the np.ndarray columns as they need to be checked in another way
    np_ndarrays = [
        "events_biosamples_type",
        "events_datasources_type",
        "events_type_administrative_databases",
        "population_recruitment_sources",
        "study_principle_investigators",
        "study_contacts",
        "study_data_collection_events",
        "study_populations",
    ]
    converted_frame = converted_df.drop(np_ndarrays, axis=1)
    check_frame = lifecycle_converted_df.drop(np_ndarrays, axis=1)

    pd.testing.assert_frame_equal(converted_frame, check_frame)

    for np_ndarray in np_ndarrays:
        # You could use np.testing.assert_array_equal for testing, however,
        # that gives an assertion error (while all seems to be the same).
        # Also assert np.array_equal with equal_nan = True does not work
        # Therefore convert the values to a list and then test with
        # assert_series_equal as it results in a type pandas.core.series.Series
        converted_ndarrays = converted_df[np_ndarray].apply(
            lambda x: list(x).sort() if x is not np.nan else x
        )
        check_ndarrays = lifecycle_converted_df[np_ndarray].apply(
            lambda x: list(x).sort() if x is not np.nan else x
        )

        pd.testing.assert_series_equal(converted_ndarrays, check_ndarrays)
        assert np.array_equal(converted_ndarrays, check_ndarrays, equal_nan=True)
        np.testing.assert_array_equal(converted_ndarrays, check_ndarrays)

    lifecycle._convert_list_values.assert_called_once()
    lifecycle._extract_data.assert_called_once()
    lifecycle._group_column_information.assert_called_once()
