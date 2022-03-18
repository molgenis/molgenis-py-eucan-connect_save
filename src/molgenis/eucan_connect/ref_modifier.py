from typing import List

import numpy as np
import pandas as pd

from molgenis.eucan_connect.errors import EucanWarning
from molgenis.eucan_connect.model import RefData
from molgenis.eucan_connect.printer import Printer


class RefModifier:
    """
    Performs checks on and if necessary converts data in the reference columns (columns
    which contain a reference to another table):
    - events_biosamples_type
    - events_datasources_type
    - events_type_administrative_databases
    - population_recruitment_sources
    """

    def __init__(self, printer: Printer, ref_data: RefData, source_data: pd.DataFrame):
        self.df = source_data
        self.ref_data = ref_data
        self.printer = printer
        self.warnings: List[EucanWarning] = list()

    def ref_modifier(self):
        """
        Verifies the reference data in a catalogue:
        1. Checks for reference values which are not yet in the EUCAN-Connect Catalogue
        2. Replaces the reference values with the right IDs
        """
        with self.printer.indentation():
            self._check_reference_data()
            self._convert_reference_data()
        return self.warnings

    def _check_reference_data(self):
        """
        Checks for the "reference" columns:
        - events_biosamples_type
        - events_datasources_type
        - events_type_administrative_databases
        - population_recruitment_sources
        if values are already in the EUCAN-Connect Catalogue, if not these will be added
        """

        self.printer.print("Check for new reference values")

        eucan_ref_columns = [
            {"events_biosamples_type": "biosamples"},
            {"events_datasources_type": "data_sources"},
            {"events_type_administrative_databases": "database_types"},
            {"population_recruitment_sources": "recruitment_sources"},
        ]

        for ref_column in eucan_ref_columns:
            col = list(ref_column.keys())[0]
            if col in self.df.columns:
                unique_refs = list(self.df[col].explode().unique())
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

    def _convert_reference_data(self):
        """
        Replaces the values in the "reference" columns:
        - events_biosamples_type
        - events_datasources_type
        - events_type_administrative_databases
        - population_recruitment_sources
        by the right IDs
        """

        self.printer.print("Replace reference values by ID")
        eucan_ref_columns = [
            "events_biosamples_type",
            "events_datasources_type",
            "events_type_administrative_databases",
            "population_recruitment_sources",
        ]

        ref_columns = set(eucan_ref_columns).intersection(self.df.columns)

        for col in ref_columns:
            self.df[col] = self.df[col].apply(
                lambda x: list(map(str.lower, x)) if x is not np.nan else x
            )
            for character in self.ref_data.invalid_id_characters():
                invalid_character = list(character.keys())[0]
                replacement = character[invalid_character]
                # list(map etc) does not work with replace
                self.df[col] = self.df[col].apply(
                    lambda x: [i.replace(invalid_character, replacement) for i in x]
                    if x is not np.nan
                    else x
                )
