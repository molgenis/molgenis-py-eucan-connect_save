from typing import List

from molgenis.client import MolgenisRequestError
from molgenis.eucan_connect.errors import (
    ErrorReport,
    EucanError,
    EucanWarning,
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
)
from molgenis.eucan_connect.printer import Printer
from molgenis.eucan_connect.ref_modifier import RefModifier


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
        self.warnings: List[EucanWarning] = []

    def import_catalogues(self, catalogues: List[Catalogue]) -> ErrorReport:
        """
        Imports data from the provided source catalogue(s) into the tables
        in the EUCAN-Connect catalogue.

        Parameters:
            catalogues (List[Catalogue]): The list of catalogues to import data from
        """

        report: ErrorReport = ErrorReport(catalogues)
        for catalogue in catalogues:
            self.warnings = []
            self.printer.print_catalogue_title(catalogue)
            try:
                self._import_catalogue(catalogue)
            except EucanError as e:
                self.printer.print_error(e)
                report.add_error(catalogue, e)

            report.add_warnings(catalogue, self.warnings)
        self.printer.print_summary(report)
        return report

    @requests_error_handler
    def _import_catalogue(self, catalogue: Catalogue):
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

        self.printer.print("✏️ Verify reference data")
        with self.printer.indentation():
            self.warnings += RefModifier(
                printer=self.printer,
                ref_data=self.ref_data,
                source_data=source_data,
            ).ref_modifier()

        # Convert the source catalogue dataframes to CatalogueData
        catalogue_data = self.session.create_catalogue_data(catalogue, source_data)

        # Import any possible new references into the EUCAN-Connect Catalogue
        self._add_new_ref_data(self.ref_data)

        # Import the data from the source catalogue to the EUCAN-Connect Catalogue
        self._import_catalogue_data(catalogue_data)

    @requests_error_handler
    def _get_lifecycle_data(self, catalogue: Catalogue):
        try:
            self.printer.print_sub_header(
                f"📥 Get data of source catalogue {catalogue.description}"
            )
            return LifeCycle(self.session, self.printer, catalogue).lifecycle_data()

        except MolgenisRequestError as e:
            raise EucanError(
                f"Error retrieving data of catalogue {catalogue.description}"
            ) from e

    def _add_new_ref_data(self, ref_data: RefData):
        """
        Inserts new reference data into the EUCAN-Connect Catalogue
        """

        self.printer.print_sub_header("📤 If there, import new reference data")
        with self.printer.indentation():
            self.warnings += Importer(
                session=self.session,
                printer=self.printer,
            ).import_reference_data(ref_data)

    def _import_catalogue_data(self, catalogue_data: CatalogueData):
        """
        Inserts the data of the source catalogue to the EUCAN-Connect Catalogue
        This happens in two phases:
        1. All source catalogue data are removed from the EUCAN-Connect Catalogue
        2. Data from the source catalogue are inserted into EUCAN-Connect Catalogue
        """
        self.printer.print_sub_header(
            f"📤 Importing source catalogue {catalogue_data.catalogue.description}"
        )
        with self.printer.indentation():
            self.warnings += Importer(
                session=self.session,
                printer=self.printer,
            ).import_catalogue_data(catalogue_data)
