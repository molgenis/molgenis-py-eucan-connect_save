from typing import Dict, List

import numpy as np
import pandas as pd
import requests
from pandas import json_normalize

from molgenis.client import BlockAll
from molgenis.eucan_connect.errors import EucanError, EucanWarning
from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.model import Catalogue, TableType
from molgenis.eucan_connect.printer import Printer


class LifeCycle:
    """
    This class is responsible for retrieving data from the source catalogue
    LifeCycle, stored at a molgenis EMX2 server and convert it to the
    EUCAN-Connect Catalogue data model.
    """

    def __init__(self, session: EucanSession, printer: Printer, catalogue: Catalogue):
        """Constructs a new Session.
        Args:
        url -- URL of the REST API. Should be of form 'http[s]://<EMX2 server>[:port]/'
        Examples:
        session = Session('https://data-catalogue.molgeniscloud.org/')
        """
        self.catalogue = catalogue
        self.eucan_session = session
        self._lc_session = requests.Session()
        self._lc_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._lc_session.cookies.policy = BlockAll()
        self.printer = printer
        self.warnings: List[EucanWarning] = []

    def lifecycle_data(self) -> pd.DataFrame:
        """
        Retrieves data from the provided source catalogue
        """
        self.printer.print(f"🗑 Get {self.catalogue.description} studies")

        # Retrieve the list with the cohorts in the source catalogue:
        lc_cohort_data = self.get_lc_cohort_data()

        if len(lc_cohort_data) == 0:
            raise EucanError(f"Number of records for {self.catalogue.description} is 0")
        else:
            self.printer.print_sub_header(
                f"Number of cohorts retrieved for {self.catalogue.description} is "
                f"{len(lc_cohort_data)}"
            )

        df_lc_cohorts = self._create_df(lc_cohort_data)
        df_lc_cohorts = self._convert_values(df_lc_cohorts)

        return df_lc_cohorts

    def get_lc_cohort_data(self):
        lc_query = """query {Cohorts {pid, name, acronym, description, startYear,
                                        endYear, website, # contactEmail,
                                contributors {contact {title {name}, firstName, prefix,
                                                        surname, email},
                                                        contributionType {name}},
                                fundingStatement, design {name}, numberOfParticipants,
                                numberOfParticipantsWithSamples,
                                supplementaryInformation, dataAccessConditions {name},
                                dataAccessConditionsDescription, designPaper {doi},
                                subcohorts {name, description, inclusionCriteria,
                                            # mainMedicalCondition {name},
                                            supplementaryInformation,
                                            ageGroups {name, code},
                                            numberOfParticipants},
                                          collectionEvents {name, description,
                                                                startYear {name},
                                                                endYear {name},
                                                                startMonth {code},
                                                                endMonth {code},
                                                         areasOfInformation {name},
                                                         dataCategories {name},
                                                         sampleCategories {name}}}}"""
        lc_url = self.catalogue.catalogue_url + "/catalogue/graphql"
        response = self._lc_session.post(
            lc_url, headers=self._lc_headers, json={"query": lc_query}
        )
        lc_data = response.json()

        return lc_data["data"]["Cohorts"]

    def _create_df(self, json_data):
        table_prefix = {
            "study": "study_",
            "collectionEvents": "events_",
            "contributors": "persons_",
            "subcohorts": "population_",
        }
        # Convert the json list to a pandas dataframe
        df_cohorts = json_normalize(json_data)

        # Rename the study columns
        df_cohorts.rename(
            columns={
                "name": "study_name",
                "description": "objectives",
                "startYear": "start_year",
                "endYear": "end_year",
                "fundingStatement": "funding",
                "design.name": "study_design",
                "numberOfParticipants": "number_of_participants",
                "numberOfParticipantsWithSamples": "participants_with_biosamples",
                "supplementaryInformation": "number_of_participants_supplement",
                "dataAccessConditionsDescription": "contact_procedures",
                "designPaper.doi": "marker_paper",
            },
            inplace=True,
        )

        # Add the study prefix
        df_cohorts = df_cohorts.add_prefix(table_prefix["study"])

        # Extract the contact information, data collection events and populations
        # As either one or more of them are missing for some cohorts exclude missing
        # rows when extracting the data
        df_cohorts = self._extract_data(json_data, df_cohorts, table_prefix)

        # Rename the columns of the extracted data
        df_cohorts.rename(
            columns={
                "persons_contact.title.name": "persons_title",
                "persons_contact.firstName": "persons_first_name",
                "persons_contact.email": "persons_email",
                "population_inclusionCriteria": "population_selection_"
                "criteria_supplement",
                "population_numberOfParticipants": "population_number_of_participants",
                "population_supplementaryInformation": "population_recruitment_"
                "sources_supplement",
                "study_pid": "study_id",
            },
            inplace=True,
        )

        return df_cohorts

    def _convert_values(self, df_converted: pd.DataFrame):
        # Define the IDs
        len_id_nr = "{:00" + str(len(str(len(df_converted))) + 1) + "}"

        df_converted["study_id"] = self.catalogue.get_id_prefix(
            TableType.STUDIES
        ) + df_converted["study_id"].str.replace(" ", "_", regex=False)
        df_converted["person"] = df_converted["persons_email"].fillna(
            df_converted["persons_first_name"] + df_converted["persons_contact.surname"]
        )
        df_converted["persons_id"] = self.catalogue.get_id_prefix(
            TableType.PERSONS
        ) + df_converted.groupby(["person"], sort=False).ngroup().apply(
            len_id_nr.format
        )
        df_converted.loc[df_converted["person"].isnull(), "persons_id"] = np.nan
        df_converted["events_id"] = self.catalogue.get_id_prefix(
            TableType.EVENTS
        ) + df_converted.groupby(
            ["study_id", "events_name"], sort=False
        ).ngroup().apply(
            len_id_nr.format
        )
        df_converted.loc[df_converted["events_name"].isnull(), "events_id"] = np.nan
        df_converted["population_id"] = self.catalogue.get_id_prefix(
            TableType.POPULATIONS
        ) + df_converted.groupby(
            ["study_id", "population_name"], sort=False
        ).ngroup().apply(
            len_id_nr.format
        )
        df_converted.loc[
            df_converted["population_name"].isnull(), "population_id"
        ] = np.nan

        # Add a prefix to the last name
        df_converted["persons_last_name"] = (
            df_converted["persons_contact.prefix"]
            .fillna("")
            .str.cat(df_converted["persons_contact.surname"], " ")
            .str.lstrip()
        )
        df_converted["events_start_end_year"] = (
            df_converted["events_startYear.name"]
            + "-"
            + df_converted["events_endYear.name"].fillna("")
        )

        # Add the study acronym to the events and population name
        df_converted["events_name"] = (
            df_converted["study_acronym"] + " - " + df_converted["events_name"]
        )
        df_converted["population_name"] = (
            df_converted["study_acronym"] + " - " + df_converted["population_name"]
        )

        # Define start/end periods
        if "events_startMonth.code" in df_converted.columns:
            if "events_endMonth.code" in df_converted.columns:
                df_converted["events_start_end_month"] = (
                    df_converted["events_startMonth.code"]
                    + "-"
                    + df_converted["events_endMonth.code"].fillna("")
                )
            else:
                df_converted["events_start_end_month"] = (
                    df_converted["events_startMonth.code"] + "-"
                )

        # Define the principal investigator / contact person, data_sources
        # and recruitment_sources
        df_converted = self._convert_list_values(df_converted)

        # Create and fill the principal investigator and contact person columns
        df_converted["temp_pi"] = np.where(
            df_converted["persons_contribution_types"].notna(),
            np.where(
                df_converted["persons_contribution_types"]
                .str.join(" ")
                .str.contains("Principal Investigator"),
                df_converted["persons_id"],
                np.nan,
            ),
            np.nan,
        )

        df_converted["temp_contacts"] = np.where(
            df_converted["persons_contribution_types"].notna(),
            np.where(
                df_converted["persons_contribution_types"]
                .str.join(" ")
                .str.contains("Contact person"),
                df_converted["persons_id"],
                np.nan,
            ),
            np.nan,
        )

        # As not all persons have a contribution type,
        # fill missing ones with type Contact Person
        df_converted["temp_contacts"] = np.where(
            df_converted["temp_pi"].isna(),
            df_converted["temp_contacts"].fillna(df_converted["persons_id"]),
            df_converted["temp_contacts"],
        )

        # Group principle investigators, contacts, events and populations per study
        df_converted = self._group_column_information(df_converted)

        # Convert study_access_possibility to bool
        df_converted["study_access_possibility"] = df_converted[
            "study_access_possibility"
        ].apply(lambda x: np.nan if type(x) is not np.ndarray else True)
        # ', '.join([str(i) for i in x]))

        # Drop irrelevant columns
        df_converted.drop(
            [
                "study_dataAccessConditions",
                "person",
                "persons_contact.prefix",
                "persons_contact.surname",
                "events_startYear.name",
                "events_endYear.name",
                "events_startMonth.code",
                "events_endMonth.code",
                "events_areasOfInformation",
                "events_dataCategories",
                "events_sampleCategories",
                "population_ageGroups",
                "persons_contributionType",
                "persons_contribution_types",
                "temp_pi",
                "temp_contacts",
            ],
            axis=1,
            inplace=True,
            errors="ignore",
        )

        return df_converted

    @staticmethod
    def _convert_list_values(df_list_conversion: pd.DataFrame) -> pd.DataFrame:
        # Convert per column the list items to columns
        list_columns = {
            "study_dataAccessConditions": ["_name", "study_access_possibility"],
            "persons_contributionType": ["_name", "persons_contribution_types"],
            "events_areasOfInformation": [
                "_name",
                "events_type_administrative_databases",
            ],
            "events_sampleCategories": ["_name", "events_biosamples_type"],
            "events_dataCategories": ["_name", "events_datasources_type"],
            "population_ageGroups": ["_code", "population_recruitment_sources"],
        }
        for df_col in list_columns.keys():
            df_converted_list = pd.DataFrame(
                [pd.Series(x) for x in df_list_conversion[df_col]]
            )
            df_converted_list.columns = [
                "list_col{}".format(int(x) + 1) for x in df_converted_list.columns
            ]

            for list_col in df_converted_list.columns:
                # The converted list item columns are dictionaries, convert to columns
                df_converted_dict = pd.DataFrame(
                    [pd.Series(x) for x in df_converted_list[list_col]]
                )
                df_converted_dict.columns = [
                    list_col + "_{}".format(x) for x in df_converted_dict.columns
                ]

                # Combine the converted columns with the original dataframe
                if len(df_list_conversion) == len(df_converted_dict):
                    df_list_conversion = df_list_conversion.join(df_converted_dict)
                else:
                    raise EucanError(
                        f"LifeCycle: _convert_list_columns: "
                        f"Columns can not be added, "
                        f"dataFrames {df_list_conversion} and "
                        f"{df_converted_dict} differ in size"
                    )

            dict_cols = [
                col + list_columns[df_col][0] for col in df_converted_list.columns
            ]

            # Combine the "list" columns to one column (list),
            # and remove missing values, empties list are set to NaN
            df_list_conversion[list_columns[df_col][1]] = df_list_conversion[
                dict_cols
            ].values.tolist()
            df_list_conversion[list_columns[df_col][1]] = df_list_conversion[
                list_columns[df_col][1]
            ].apply(lambda l: pd.Series(l).dropna().values)
            df_list_conversion[list_columns[df_col][1]] = df_list_conversion[
                list_columns[df_col][1]
            ].apply(lambda x: np.nan if len(x) == 0 else x)

            # Drop columns that won't be used
            for var in ["_0", "_code", "_name"]:
                cols = [col + var for col in df_converted_list.columns]
                df_list_conversion.drop(cols, axis=1, inplace=True, errors="ignore")

        return df_list_conversion

    @staticmethod
    def _extract_data(json_data: List[dict], df_in: pd.DataFrame, table_prefix: Dict):
        df_extracted = df_in
        for var in ["contributors", "collectionEvents", "subcohorts"]:
            df_no_nan = df_in.dropna(subset=[table_prefix["study"] + var])
            row_list = df_no_nan.index
            df_add = pd.DataFrame
            for row_index in row_list:
                df_row = json_normalize(
                    json_data[row_index],
                    meta="pid",
                    meta_prefix=table_prefix["study"],
                    record_path=var,
                    record_prefix=table_prefix[var],
                )
                if row_index == row_list[0]:
                    df_add = df_row
                else:
                    df_add = df_add.append(df_row, ignore_index=True)

            df_extracted = pd.merge(df_extracted, df_add, on="study_pid", how="outer")
            df_extracted.drop([table_prefix["study"] + var], axis=1, inplace=True)
        return df_extracted

    @staticmethod
    def _group_column_information(df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to combine the column information of the same study in multiple rows
        into one column
        :param df:
        :return a pandas DataFrame:
        """
        columns = {
            "temp_pi": "study_principle_investigators",
            "temp_contacts": "study_contacts",
            "events_id": "study_data_collection_events",
            "population_id": "study_populations",
        }

        for column in columns.keys():
            df_grouped = df.groupby(["study_id"], as_index=False).agg({column: list})
            df_grouped.rename(columns={column: columns[column]}, inplace=True)
            df = pd.merge(df, df_grouped, on="study_id", how="outer")

            # Remove duplicates and missing values from the list
            df[columns[column]] = df[columns[column]].apply(lambda x: list(set(x)))
            df[columns[column]] = df[columns[column]].apply(
                lambda l: pd.Series(l).dropna().values
            )
            df[columns[column]] = df[columns[column]].apply(
                lambda x: np.nan if len(x) == 0 else x
            )

        return df
