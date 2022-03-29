from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from molgenis.eucan_connect.errors import EucanError, EucanWarning
from molgenis.eucan_connect.model import Catalogue


@pytest.fixture
def importer_init():
    with patch("molgenis.eucan_connect.eucan.Importer") as importer_mock:
        yield importer_mock


@pytest.fixture
def lifecycle_init():
    with patch("molgenis.eucan_connect.eucan.LifeCycle") as lifecycle_mock:
        yield lifecycle_mock


@pytest.fixture
def ref_modifier_init():
    with patch("molgenis.eucan_connect.eucan.RefModifier") as ref_modifier_mock:
        yield ref_modifier_mock


def test_import_catalogues(
    eucan,
    lifecycle_init,
    ref_modifier_init,
    importer_init,
    fake_source_data,
    fake_catalogue_data,
    meta_data,
):
    lc = Catalogue("LC", "LifeCycle", "lifecycle_url", "LifeCycle")
    lc_catalogue_data = fake_catalogue_data
    lc_source_data = fake_source_data
    warning = EucanWarning("warning")
    eucan.session.get_meta = MagicMock(return_value=meta_data)
    lifecycle_init.return_value.lifecycle_data.side_effect = [lc_source_data]
    ref_modifier_init.return_value.ref_modifier.side_effect = [[warning], []]
    importer_init.return_value.import_reference_data.side_effect = [[warning], []]
    importer_init.return_value.import_catalogue_data.side_effect = [[warning], []]

    eucan.session.create_catalogue_data = MagicMock()
    eucan.session.create_catalogue_data.return_value = lc_catalogue_data
    eucan._add_new_ref_data = MagicMock(side_effect=eucan._add_new_ref_data)
    eucan._import_catalogue_data = MagicMock(side_effect=eucan._import_catalogue_data)

    report = eucan.import_catalogues([lc])

    assert eucan.printer.print_catalogue_title.mock_calls == [mock.call(lc)]
    assert eucan.printer.print_sub_header.mock_calls[0] == mock.call(
        "📥 Get data of source catalogue LifeCycle"
    )
    assert lifecycle_init.mock_calls == [
        mock.call(eucan.session, eucan.printer, lc),
        mock.call().lifecycle_data(),
    ]

    assert eucan.printer.print.mock_calls[0] == mock.call("✏️ Verify reference data")

    assert ref_modifier_init.mock_calls == [
        mock.call(
            printer=eucan.printer, ref_data=eucan.ref_data, source_data=lc_source_data
        ),
        mock.call().ref_modifier(),
    ]
    eucan.session.create_catalogue_data.assert_called_once()

    # pd.testing.assert_frame_equal(
    #     # [0] = *args, [0][1] = second positional arg to my function
    #     eucan.session.create_catalogue_data.call_args[0][1],
    #     lc_source_data)

    eucan._add_new_ref_data.assert_called_once_with(eucan.ref_data)
    eucan._import_catalogue_data.assert_called_once_with(lc_catalogue_data)

    assert importer_init.mock_calls == [
        mock.call(printer=eucan.printer, session=eucan.session),
        mock.call().import_reference_data(eucan.ref_data),
        mock.call(printer=eucan.printer, session=eucan.session),
        mock.call().import_catalogue_data(lc_catalogue_data),
    ]

    assert lc not in report.errors

    # assert report.warnings[lc] == [warning]
    eucan.printer.print_summary.assert_called_once_with(report)


def test_import_catalogues_fails(eucan):
    eucan.import_catalogues = MagicMock(side_effect=EucanError("Something went wrong"))
    catalogue = Catalogue("LC", "LifeCycle", "lifecycle_url", "LifeCycle")

    with pytest.raises(EucanError) as e:
        eucan.import_catalogues(catalogue)

    assert str(e.value) == "Something went wrong"


def test_catalogue_no_module(eucan):
    bc = Catalogue("Test", "Test", "test_url", "BirthCohorts")
    mica = Catalogue("Test", "Test", "test_url", "Mica")
    unk = Catalogue("Test", "Test", "test_url", "DNA_catalogue")
    report = eucan.import_catalogues([bc, mica, unk])
    assert str(report.errors[bc]) == str(
        EucanError("Birth cohort data. No module available yet!")
    )
    assert str(report.errors[mica]) == str(
        EucanError("Mica data. No module available yet!")
    )
    assert str(report.errors[unk]) == str(
        EucanError("Unknown catalogue type DNA_catalogue")
    )
