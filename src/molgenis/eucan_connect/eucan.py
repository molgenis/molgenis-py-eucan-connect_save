from typing import List

import numpy as np
import pandas as pd

from molgenis.client import MolgenisRequestError
from molgenis.eucan_connect.errors import (
    ErrorReport,
    EucanError,
    requests_error_handler,
)
from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.importer import Importer
from molgenis.eucan_connect.lifecycle import LifeCycle
from molgenis.eucan_connect.model import (
    Catalogue,
    CatalogueData,
    IsoCountryData,
    RefData,
    Table,
    TableType,
)
from molgenis.eucan_connect.printer import Printer


class Eucan:
    """
    Main class for importing data from source catalogues
    into the EUCAN-Connect Catalogue.
    """

    def __init__(self, session: EucanSession):
        """
        :param EucanSession session: an authenticated session with
                                     an EUCAN-Connect Catalogue
        """
        self.session = session
        self.printer = Printer()
        self.iso_country_data: IsoCountryData = session.get_iso_country_data()
        self.ref_data: RefData = session.get_reference_data()

    def import_catalogues(self, catalogues: List[Catalogue]) -> ErrorReport:
        """
        Imports data from the provided source catalogue(s) into the tables
        in the EUCAN-Connect catalogue.

        Parameters:
            catalogues (List[Catalogue]): The list of catalogues to import data from
        """
        report = ErrorReport(catalogues)
        importer = Importer(self.session, self.printer)
        for catalogue in catalogues:
            self.printer.print_catalogue_title(catalogue)
            try:
                self._import_catalogue(catalogue, report, importer)
            except EucanError as e:
                self.printer.print_error(e)
                report.add_error(catalogue, e)

        self.printer.print_summary(report)
        return report

    @requests_error_handler
    def _import_catalogue(
        self, catalogue: Catalogue, report: ErrorReport, importer: Importer
    ):
        # Get the data from the source catalogue(s)
        if catalogue.catalogue_type == "BirthCohorts":
            # Get the data from the source catalogue type birth cohorts
            raise EucanError("Birth cohort data. No module available yet!")
        elif catalogue.catalogue_type == "LifeCycle":
            # Get the data from the source catalogue type LifeCycle
            source_data = self._get_lifecycle_data(catalogue)
        elif catalogue.catalogue_type == "Mica":
            # Get the data from the source catalogue type Mica
            raise EucanError("Mica data. No module available yet!")
        else:
            raise EucanError(f"Unknown catalogue type {catalogue.catalogue_type}")

        # Check (and if necessary convert) reference data
        source_data = self._check_reference_data(source_data)

        source_data = self._convert_reference_data(source_data)

        # Convert the source catalogue dataframes to CatalogueData
        catalogue_data = self._create_catalogue_data(catalogue, source_data)

        # Import any possible new references into the EUCAN-Connect Catalogue
        self._add_new_ref_data(self.ref_data, catalogue, importer, report)

        # Import the data from the source catalogue to the EUCAN-Connect Catalogue
        self._import_catalogue_data(catalogue_data, importer, report)

    @requests_error_handler
    def _get_lifecycle_data(self, catalogue: Catalogue):
        try:
            self.printer.print_sub_header(
                f"📥 Get data of source catalogue {catalogue.description}"
            )
            return LifeCycle(self.session, self.printer).lifecycle_data(catalogue)

        except MolgenisRequestError as e:
            raise EucanError(
                f"Error retrieving data of catalogue {catalogue.description}"
            ) from e

    def _add_new_ref_data(
        self,
        ref_data: RefData,
        catalogue: Catalogue,
        importer: Importer,
        report: ErrorReport,
    ):
        """
        Inserts new reference data into the EUCAN-Connect Catalogue
        """

        self.printer.print_sub_header("📤 If there, import new reference data")
        self.printer.indent()

        warnings = importer.import_reference_data(ref_data)
        report.add_warnings(catalogue, warnings)

        self.printer.dedent()

    def _check_reference_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Checks for the "reference" columns:
        - events_biosamples_type
        - events_datasources_type
        - events_type_administrative_databases
        - population_recruitment_sources
        if values are already in the EUCAN-Connect Catalogue, if not these will be added

        :param df:
        :return: pandas DataFrame
        """

        eucan_ref_columns = [
            {"events_biosamples_type": "biosamples"},
            {"events_datasources_type": "data_sources"},
            {"events_type_administrative_databases": "database_types"},
            {"population_recruitment_sources": "recruitment_sources"},
        ]

        for ref_column in eucan_ref_columns:
            col = list(ref_column.keys())[0]
            if col in df.columns:
                unique_refs = list(df[col].explode().unique())
                if np.nan in unique_refs:
                    unique_refs.remove(np.nan)
                for ref_description in unique_refs:
                    ref_id = ref_description.lower()
                    for character in self.ref_data.invalid_id_characters():
                        invalid_character = list(character.keys())[0]
                        replacement = character[invalid_character]
                        ref_id = ref_id.replace(invalid_character, replacement)
                    if ref_id not in self.ref_data.all_refs(ref_column[col]):
                        self.ref_data.add_new_ref(
                            ref_column[col], ref_id, ref_description
                        )
                        self.printer.print(
                            f"A new reference value ({ref_description}) will be added "
                            f"for {col} in the EUCAN-Connect Catalogue"
                        )
        return df

    def _convert_reference_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces the values in the "reference" columns:
        - events_biosamples_type
        - events_datasources_type
        - events_type_administrative_databases
        - population_recruitment_sources
        by the right IDs

        :param df:
        :return: pandas DataFrame
        """

        eucan_ref_columns = [
            "events_biosamples_type",
            "events_datasources_type",
            "events_type_administrative_databases",
            "population_recruitment_sources",
        ]

        ref_columns = list(
            set(eucan_ref_columns).difference(
                list(set(eucan_ref_columns).difference(df.columns))
            )
        )

        for col in ref_columns:
            df[col] = df[col].apply(
                lambda x: list(map(str.lower, x)) if x is not np.nan else x
            )
            for character in self.ref_data.invalid_id_characters():
                invalid_character = list(character.keys())[0]
                replacement = character[invalid_character]
                # list(map etc) does not work with replace
                df[col] = df[col].apply(
                    lambda x: [i.replace(invalid_character, replacement) for i in x]
                    if x is not np.nan
                    else x
                )

        return df

    def _create_catalogue_data(
        self, catalogue: Catalogue, df_in: pd.DataFrame
    ) -> CatalogueData:
        """
        Converts processed source catalogue data to the EUCAN-Catalogue format
        Fills the four EUCAN-Connect tables for the specific source catalogue

        :param df_in: the source catalogue data in pandas DataFrame
        :return: a CatalogueData object
        """

        tables = dict()
        for table_type in TableType.get_import_order():
            id_ = table_type.base_id
            table = table_type.table
            meta = self.session.get_meta(id_)

            tables[table_type] = Table.of(
                table_type=table_type,
                meta=meta,
                rows=self.get_uploadable_data(catalogue, df_in, table),
            )

        return CatalogueData.from_dict(
            catalogue=catalogue, source=catalogue.description, tables=tables
        )

    def get_uploadable_data(
        self, catalogue: Catalogue, df_data: pd.DataFrame, table_type: str
    ) -> List[dict]:
        """
        Returns all the rows of an entity type in the dataFrame, transformed to
        the uploadable format.
        """

        table_columns = [x for x in df_data.columns if table_type in x[:10]]
        table_data = df_data[table_columns].to_dict("records")
        # Remove the "table" name from the columns and remove missing values
        for row in table_data:
            for tab_column in table_columns:
                column = tab_column.replace(table_type + "_", "", 1)
                row[column] = row[tab_column]
                del row[tab_column]

                if type(row[column]) is np.ndarray:
                    row[column] = list(row[column])
                # A NaN implemented following the standard, is the only value for which
                # the inequality comparison with itself should return True:
                if row[column] != row[column]:
                    del row[column]

        # Remove duplicate records
        unique_data = [
            i for n, i in enumerate(table_data) if i not in table_data[n + 1 :]
        ]

        # Remove empty records
        while {} in unique_data:
            unique_data.remove({})

        # Add the source catalogue
        unique_data = [
            dict(item, source_catalogue=catalogue.code) for item in unique_data
        ]

        return unique_data

    def _import_catalogue_data(
        self, catalogue_data: CatalogueData, importer: Importer, report: ErrorReport
    ):
        """
        Inserts the data of the source catalogue to the EUCAN-Connect Catalogue
        This happens in two phases:
        1. All source catalogue data are removed from the EUCAN-Connect Catalogue
        2. Data from the source catalogue are inserted into EUCAN-Connect Catalogue
        """
        self.printer.print_sub_header(
            f"📤 Importing source catalogue {catalogue_data.catalogue.description}"
        )
        self.printer.indent()

        warnings = importer.import_catalogue_data(catalogue_data)
        report.add_warnings(catalogue_data.catalogue, warnings)

        self.printer.dedent()
