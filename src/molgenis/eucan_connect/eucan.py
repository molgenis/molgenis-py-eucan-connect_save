from typing import List

from molgenis.client import MolgenisRequestError
from molgenis.eucan_connect.errors import (
    ErrorReport,
    EucanError,
    requests_error_handler,
)
from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.importer import Importer
from molgenis.eucan_connect.mica import Mica
from molgenis.eucan_connect.model import (
    Catalogue,
    CatalogueData,
    IsoCountryData,
    RefData,
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
        elif catalogue.catalogue_type == "Mica":
            # Get the data from the source catalogue type Mica
            catalogue_data = self._get_mica_data(catalogue)
        else:
            raise EucanError(f"Unknown catalogue type {catalogue.catalogue_type}")

        # Import any possible new references into the EUCAN-Connect Catalogue
        self._add_new_ref_data(self.ref_data, importer, report)

        # Import the data from the source catalogue to the EUCAN-Connect Catalogue
        self._import_catalogue_data(catalogue_data, importer, report)

    @requests_error_handler
    def _get_mica_data(self, catalogue: Catalogue):
        try:
            self.printer.print_sub_header(
                f"📥 Get data of source catalogue {catalogue.description}"
            )
            return Mica(
                self.session, self.iso_country_data, self.ref_data, self.printer
            ).mica_data(catalogue)

        except MolgenisRequestError as e:
            raise EucanError(
                f"Error retrieving data of catalogue {catalogue.description}"
            ) from e

    def _add_new_ref_data(
        self, ref_data: RefData, importer: Importer, report: ErrorReport
    ):
        """
        Inserts new reference data into the EUCAN-Connect Catalogue
        """
        self.printer.print_sub_header("📤 Importing new reference data")
        self.printer.indent()

        warnings = importer.import_data(ref_data, "RefData")
        report.add_warnings(ref_data.ref_entity, warnings)

        self.printer.dedent()

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

        warnings = importer.import_data(catalogue_data, "CatalogueData")
        report.add_warnings(catalogue_data.catalogue, warnings)

        self.printer.dedent()
