from unittest import mock
from unittest.mock import MagicMock

import pytest

from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.model import Catalogue


def test_get_catalogue_list(session):
    eucan_session = EucanSession("url")
    eucan_session.get = session.get
    eucan_session._validate_codes = MagicMock(side_effect=eucan_session._validate_codes)
    eucan_session._to_catalogues = MagicMock(side_effect=eucan_session._to_catalogues)
    catalogues = eucan_session.get_catalogues(["TC"])

    assert catalogues == [Catalogue("TC", "Test Catalogue", "https://test.nl", "Test")]

    eucan_session._validate_codes.assert_called_once()
    eucan_session._to_catalogues.assert_called_once()


def test_get_catalogue_all(session):
    eucan_session = EucanSession("url")
    eucan_session.get = session.get
    eucan_session._validate_codes = MagicMock(side_effect=eucan_session._validate_codes)
    eucan_session._to_catalogues = MagicMock(side_effect=eucan_session._to_catalogues)
    catalogues = eucan_session.get_catalogues()

    assert catalogues == [
        Catalogue("TC", "Test Catalogue", "https://test.nl", "BirthCohorts"),
        Catalogue("C2", "Test Catalogue2", "https://test2.nl", "Mica"),
        Catalogue("C3", "Test Catalogue3", "https://test3.nl", "DNA_catalogue"),
    ]

    eucan_session._validate_codes.assert_not_called()
    eucan_session._to_catalogues.assert_called_once()


def test_get_catalogue_error(session):
    eucan_session = EucanSession("url")
    eucan_session.get = session.get
    eucan_session._validate_codes = MagicMock(side_effect=eucan_session._validate_codes)
    eucan_session._to_catalogues = MagicMock(side_effect=eucan_session._to_catalogues)

    with pytest.raises(KeyError) as e:
        eucan_session.get_catalogues(["AB", "TC"])

    assert str(e.value) == "'Unknown code: AB'"

    eucan_session._validate_codes.assert_called()
    eucan_session._to_catalogues.assert_not_called()


def test_create_catalogue_data(
    session, fake_catalogue_data, fake_converted_source_data
):
    catalogue = Catalogue("Test", "succeeds", "test_url", "CatalogueType")
    converted_source_data = fake_converted_source_data
    check_cat_data = fake_catalogue_data
    eucan_session = EucanSession("url")
    eucan_session.get_meta = session.get_meta
    eucan_session._get_uploadable_data = MagicMock(
        side_effect=eucan_session._get_uploadable_data
    )

    catalogue_data = eucan_session.create_catalogue_data(
        catalogue, converted_source_data
    )
    assert catalogue_data == check_cat_data

    assert eucan_session._get_uploadable_data.call_count == 4

    assert eucan_session._get_uploadable_data.mock_calls == [
        mock.call(catalogue, converted_source_data, "events"),
        mock.call(catalogue, converted_source_data, "persons"),
        mock.call(catalogue, converted_source_data, "population"),
        mock.call(catalogue, converted_source_data, "study"),
    ]
