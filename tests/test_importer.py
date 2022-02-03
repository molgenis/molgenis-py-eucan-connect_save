from unittest import mock
from unittest.mock import MagicMock

from molgenis.eucan_connect.errors import EucanWarning
from molgenis.eucan_connect.model import Catalogue, RefEntity, TableMeta, TableType

# ToDo: Add "in case it fails" / error tests?


def test_import_catalogue(
    importer,
    fake_catalogue_data,
    session,
    printer,
):
    importer._delete_rows = MagicMock()
    existing_catalogue_data = MagicMock()
    events = MagicMock()
    persons = MagicMock()
    populations = MagicMock()
    studies = MagicMock()
    existing_catalogue_data.table_by_type = {
        TableType.EVENTS: events,
        TableType.PERSONS: persons,
        TableType.POPULATIONS: populations,
        TableType.STUDIES: studies,
    }
    catalogue_data = fake_catalogue_data
    importer._get_eucan_ids = MagicMock()

    importer.import_catalogue_data(catalogue_data)

    # Ik zou verwachten dat deze meerdere keren werd gecalled, maar zegt 0 keer
    # assert importer._get_eucan_ids.assert_called_once()

    assert session.add_batched.mock_calls == [
        mock.call(catalogue_data.persons.type.base_id, catalogue_data.persons.rows),
        mock.call(catalogue_data.events.type.base_id, catalogue_data.events.rows),
        mock.call(
            catalogue_data.populations.type.base_id, catalogue_data.populations.rows
        ),
        mock.call(catalogue_data.studies.type.base_id, catalogue_data.studies.rows),
    ]

    assert importer._delete_rows.mock_calls == [
        mock.call(catalogue_data.studies, catalogue_data.catalogue),
        mock.call(catalogue_data.populations, catalogue_data.catalogue),
        mock.call(catalogue_data.events, catalogue_data.catalogue),
        mock.call(catalogue_data.persons, catalogue_data.catalogue),
    ]


def test_import_references(
    importer,
    ref_data,
    session,
    printer,
):
    ref_data.add_new_ref("biosamples", "New_biosample", "Test add new biosample")
    ref_data.add_new_ref("data_sources", "New_datasource", "Test add new datasource")
    ref_data.add_new_ref("database_types", "New_database_type", "Test add new db type")
    ref_data.add_new_ref(
        "recruitment_sources", "New_recr_source", "Test new recruitment source"
    )
    session.get_meta = MagicMock()
    meta_data = TableMeta(
        meta={
            "links": {"self": "https://test_url"},
            "data": {
                "id": "eucan_biosamples",
                "label": "BioSamples",
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
    session.get_meta.return_value = meta_data

    importer.import_reference_data(ref_data)

    assert session.add_batched.mock_calls == [
        mock.call(
            RefEntity.BIOSAMPLES.base_id,
            [{"id": "New_biosample", "label": "Test add new biosample"}],
        ),
        mock.call(
            RefEntity.DATASOURCES.base_id,
            [{"id": "New_datasource", "label": "Test add new datasource"}],
        ),
        mock.call(
            RefEntity.DATABASETYPES.base_id,
            [{"id": "New_database_type", "label": "Test add new db type"}],
        ),
        mock.call(
            RefEntity.RECRUITMENTSOURCES.base_id,
            [{"id": "New_recr_source", "label": "Test new recruitment source"}],
        ),
    ]


def test_delete_rows(importer, session, fake_catalogue_data):
    catalogue = Catalogue("Test", "Test catalogue", "test_url", "Source catalogue")
    importer._delete_rows(fake_catalogue_data.persons, catalogue)

    assert importer.printer.print.mock_calls == [
        mock.call("Deleting 2 rows in eucan_persons")
    ]

    assert importer.warnings == [
        EucanWarning(
            "This Test catalogue eucan_persons ID person_deleted_id "
            "is not in the source catalogue anymore."
        )
    ]

    # A simple assert_called_with does not work as the order of
    # ["person_deleted_id", "person_id"] in the mock can differ per run and the test
    # is therefore not consistent

    assert session.delete_list.call_count == 1

    received_args = session.delete_list.call_args
    target_arg = received_args[0]
    assert target_arg[0] == "eucan_persons"
    assert target_arg[1].sort() == ["person_deleted_id", "person_id"].sort()
