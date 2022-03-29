import pytest
import requests.exceptions

from molgenis.eucan_connect.errors import (
    ErrorReport,
    EucanError,
    EucanWarning,
    requests_error_handler,
)
from molgenis.eucan_connect.model import Catalogue


def test_warning():
    warning = EucanWarning("test")
    assert warning.message == "test"


def test_error():
    error = EucanError("test")
    assert str(error) == "test"


def test_error_report():
    a = Catalogue("A", "A", "url", "type")
    b = Catalogue("B", "B", "url", "type")
    report = ErrorReport([a, b])
    warning = EucanWarning("warning")
    error = EucanError("error")

    assert not report.has_errors()
    assert not report.has_warnings()

    report.add_error(a, error)

    assert report.errors[a] == error
    assert b not in report.errors
    assert report.has_errors()
    assert not report.has_warnings()

    report.add_warnings(b, [warning, warning])

    assert report.warnings[b] == [warning, warning]
    assert a not in report.warnings
    assert report.has_errors()
    assert report.has_warnings()


def test_requests_error_handler():
    exception = requests.exceptions.ConnectionError()

    @requests_error_handler
    def raising_function():
        raise exception

    with pytest.raises(EucanError) as exception_info:
        raising_function()

    assert exception_info.value.__cause__ == exception
