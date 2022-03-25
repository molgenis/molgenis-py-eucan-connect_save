"""
Example usage file meant for development.
Make sure you have an .env file in this folder.
"""

from dotenv import dotenv_values

from molgenis.eucan_connect.eucan import Eucan
from molgenis.eucan_connect.eucan_client import EucanSession

# Get credentials from .env
config = dotenv_values(".env")
target = config["TARGET"]
username = config["USERNAME"]
password = config["PASSWORD"]

# Login to the EUCAN-Connect Catalogue with an EucanSession
session = EucanSession(url=target)
session.login(username, password)

# Get the catalogue(s) you want to import
catalogues = session.get_catalogues(["BC", "RC", "MS", "LC"])

# Instantiate the Eucan class and import the catalogue(s)
eucan = Eucan(session)
import_report = eucan.import_catalogues(catalogues)
