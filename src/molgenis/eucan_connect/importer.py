from typing import List, Set

from molgenis.client import MolgenisRequestError
from molgenis.eucan_connect.errors import EucanError, EucanWarning
from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.model import Catalogue, CatalogueData, RefData, Table
from molgenis.eucan_connect.printer import Printer


class Importer:
    """
    This class is responsible for uploading the data into the EUCAN-Connect Catalogue
    """

    def __init__(self, session: EucanSession, printer: Printer):
        self.session = session
        self.printer = printer
        self.warnings: List[EucanWarning] = []

    def import_catalogue_data(
        self, catalogue_data: CatalogueData
    ) -> List[EucanWarning]:
        """
        Inserts the data of the source catalogue into the EUCAN-Connect Catalogue
        This happens in two steps:
        1. All source catalogue data are removed from the EUCAN-Connect Catalogue
        2. Data from the source catalogue is inserted into EUCAN-Connect Catalogue
        :param catalogue_data: CatalogueData object
        :return: List with warnings
        """
        self.warnings = []
        self.printer.indent()
        for table in reversed(catalogue_data.import_order):
            try:
                self._delete_rows(table, catalogue_data.catalogue)
            except MolgenisRequestError as e:
                raise EucanError(
                    f"Error deleting existing rows from {table.type.base_id}"
                ) from e

        for table in catalogue_data.import_order:
            self.printer.print(
                f"Importing {len(table.rows)} rows in {table.type.base_id}"
            )
            try:
                self.session.add_batched(table.type.base_id, table.rows)
            except MolgenisRequestError as e:
                raise EucanError(f"Error importing rows to {table.type.base_id}") from e
        self.printer.dedent()

        return self.warnings

    def import_reference_data(self, reference_data: RefData) -> List[EucanWarning]:
        """
        Inserts the new reference data into the EUCAN-Connect Catalogue
        Existing data is retrieved and with that a set with new references is created.

        :param reference_data: RefData object
        :return: List with warnings
        """
        self.printer.indent()
        for table_type in reference_data.table_by_type:
            entity_type_id = table_type.base_id
            meta = self.session.get_meta(entity_type_id)
            id_attr = meta.id_attribute
            existing_data = self.session.get(
                entity_type_id, batch_size=10000, attributes=id_attr
            )
            existing_ids = {row[id_attr] for row in existing_data}
            # Based on the existing identifiers, decide which rows should be added
            add = list()
            for reference in reference_data.table_by_type[table_type].rows:
                if reference[id_attr] not in existing_ids:
                    add.append(reference)

            if len(add) > 0:
                self.printer.print(
                    f"{len(add)} new row(s) will be imported " f"in {entity_type_id}"
                )
            else:
                self.printer.print(
                    f"No new data needs to be imported " f"in {entity_type_id}"
                )
            try:
                self.session.add_batched(entity_type_id, add)
            except MolgenisRequestError as e:
                raise EucanError(f"Error importing rows to {entity_type_id}") from e
        self.printer.dedent()

        return self.warnings

    def _delete_rows(self, table: Table, catalogue: Catalogue):
        """
        Deletes all rows from an EUCAN-Connect Catalogue table
        from the source catalogue of which data is imported.

        :param Table table: the table containing the converted source catalogue data
        :catalogue Catalogue catalogue: the source catalogue that is being imported
        """
        # Compare the ids from the source catalogue and the EUCAN-Connect Catalogue
        # to see what data are deleted
        source_ids = {row["id"] for row in table.rows}
        eucan_ids = self._get_eucan_ids(table, catalogue)
        deleted_ids = eucan_ids.difference(source_ids)

        # Show a warning for every id that is not in the source catalogue anymore
        for id_ in deleted_ids:
            warning = EucanWarning(
                f"This {catalogue.description} {table.type.base_id} ID {id_} is not "
                f"in the source catalogue anymore."
            )
            self.printer.print_warning(warning)
            self.warnings.append(warning)

        # Delete the existing source catalogue rows in the EUCAN-Connect Catalogue
        if eucan_ids:
            self.printer.print(
                f"Deleting {len(eucan_ids)} rows in {table.type.base_id}"
            )
            self.session.delete_list(table.type.base_id, list(eucan_ids))

    def _get_eucan_ids(self, table: Table, catalogue: Catalogue) -> Set[str]:
        try:
            rows = self.session.get(
                table.type.base_id, batch_size=10000, attributes="id,source_catalogue"
            )
        except MolgenisRequestError as e:
            raise EucanError(f"Error getting rows from {table.type.base_id}") from e

        return {
            row["id"]
            for row in rows
            if row.get("source_catalogue", {}).get("id", "") == catalogue.code
        }
