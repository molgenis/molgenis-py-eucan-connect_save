# from molgenis.eucan_connect.errors import EucanError #, EucanWarning, EucanReport
from molgenis.eucan_connect.errors import ErrorReport, EucanError, EucanWarning
from molgenis.eucan_connect.model import Catalogue


class Printer:
    """
    Simple printer that keeps track of indentation levels. Also has utility methods
    for printing some EUCAN-Connect objects.
    """

    def __init__(self):
        self.indents = 0

    def indent(self):
        self.indents += 1

    def dedent(self):
        self.indents = max(0, self.indents - 1)

    def reset_indent(self):
        self.indents = 0

    def print(self, value: str = None):
        if value:
            print(f"{'    ' * self.indents}{value}")
        else:
            print()

    def print_catalogue_title(self, catalogue: Catalogue):
        title = f"🌍 Source catalogue {catalogue.description} ({catalogue.code})"
        border = "=" * (len(title) + 1)
        self.reset_indent()
        self.print()
        self.print(border)
        self.print(title)
        self.print(border)

    def print_sub_header(self, text: str):
        self.print()
        self.print(text)

    def print_error(self, error: EucanError):
        message = str(error)
        if error.__cause__:
            message += f" - Cause: {str(error.__cause__)}"
        self.print(f"❌ {message}")

    def print_warning(self, warning: EucanWarning):
        self.print(f"⚠️ {warning.message}")

    def print_summary(self, report: ErrorReport):
        self.reset_indent()
        self.print()
        self.print("==========")
        self.print("📋 Summary")
        self.print("==========")

    #     for source in report.sources:
    #         if source in report.errors:
    #             message = f"❌ Source catalogue {source.Source} failed"
    #             if source in report.warnings:
    #                 message += f" with {len(report.warnings[node])} warning(s)"
    #         elif source in report.warnings:
    #             message = (
    #                 f"⚠️ Source catalogue {source.Source} finished successfully with "
    #                 f"{len(report.warnings[node])} warning(s)"
    #             )
    #         else:
    #             message = f"✅ Source catalogue {source.Source} finished successfully"
    #         self.print(message)
