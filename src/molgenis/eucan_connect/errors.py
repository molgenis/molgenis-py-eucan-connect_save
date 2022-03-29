from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List

import requests

from molgenis.eucan_connect.model import Catalogue


@dataclass(frozen=True)
class EucanWarning:
    """
    Class that contains a warning message. Use this when a problem occurs that
    shouldn't cancel the current action (for example converting a source catalogue
    to the EUCAN-Connect Catalogue model).
    """

    message: str


class EucanError(Exception):
    """
    Raise this exception when an error occurs that we can not recover from.
    """

    pass


@dataclass
class ErrorReport:
    """
    Summary object. Stores errors and warnings that occurred for each source catalogue.
    """

    catalogues: List[Catalogue]
    errors: DefaultDict[Catalogue, EucanError] = field(
        default_factory=lambda: defaultdict(list)
    )
    warnings: DefaultDict[Catalogue, List[EucanWarning]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_error(self, catalogue: Catalogue, error: EucanError):
        self.errors[catalogue] = error

    def add_warnings(self, catalogue: Catalogue, warnings: List[EucanWarning]):
        if warnings:
            self.warnings[catalogue].extend(warnings)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


def requests_error_handler(func):
    """
    Decorator that catches RequestExceptions and wraps them in an EucanError.
    """

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            raise EucanError("Request failed") from e

    return inner_function
