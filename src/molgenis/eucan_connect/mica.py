import json
from typing import List

import requests

from molgenis.client import BlockAll
from molgenis.eucan_connect.errors import EucanError, EucanWarning
from molgenis.eucan_connect.eucan_client import EucanSession
from molgenis.eucan_connect.model import (
    Catalogue,
    CatalogueData,
    IsoCountryData,
    RefData,
    Table,
    TableType,
)
from molgenis.eucan_connect.printer import Printer


class Mica:
    """
    This class is responsible for retrieving data from the source catalogues
    stored at a Mica server and convert it to the EUCAN-Connect Catalogue data model.
    """

    def __init__(
        self,
        session: EucanSession,
        iso_countries: IsoCountryData,
        ref_data: RefData,
        printer: Printer,
    ):
        """Constructs a new Session.
        Args:
        url -- URL of the REST API. Should be of form 'http[s]://<mica server>[:port]/'
        Examples:
        session = Session('https://mica-demo.obiba.org/')
        """
        self.iso_countries = iso_countries
        self.eucan_session = session
        self._mica_session = requests.Session()
        self._mica_headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        self._mica_session.cookies.policy = BlockAll()
        self.printer = printer
        self.ref_data = ref_data
        self.studyMrefs = {}
        self.warnings: List[EucanWarning] = []

    def mica_data(self, catalogue: Catalogue) -> CatalogueData:
        """
        Retrieves data from the provided source catalogue
        Data categories are: dataset, network, study and variable.

        The study data category contains, besides study information, also
        person, data collection events and population data.
        For the conversion to the EUCAN-Connect catalogue only data
        from the study category is selected as the other three data categories
        can not be mapped to the EUCAN-Connect Catalogue data model.
        """

        self.catalogue = catalogue
        self.printer.print(f"🗑 Get {self.catalogue.description} studies")

        # First retrieve the list with the studies in the source catalogue:
        mica_studies = self.get_mica_study_ids()
        print("aantal studies", len(mica_studies))

        # Secondly retrieve the full study data
        mica_study_data = self.get_mica_study_data(mica_studies)

        if len(mica_studies) != len(mica_study_data):
            # raise EucanError
            warning = EucanWarning(
                f"Number of studies with data ({len(mica_study_data)}) "
                f"does not equal the total number of studies ({len(mica_studies)})"
            )
            self.printer.print_warning(warning)
            self.warnings.append(warning)

        # Convert the mica study data to the EUCAN-Connect Catalogue model:
        catalogue_data = self._convert_studies_to_eucan(mica_study_data)

        # Add the study mrefs to the data
        self.printer.print(
            "Adding the event, person and population mrefs to the studies"
        )
        catalogue_data = self.eucan_session.set_study_mrefs(
            catalogue_data, self.studyMrefs
        )

        return catalogue_data

    def get_mica_study_ids(self, start: int = 0, batch_size: int = 100) -> List:
        """
        Get a list with study IDs from a catalogue by using the Mica REST API Client.
        The REST API Client that is used here returns not only the IDs,
        but also study information. It however turns out that this is
        'summary' information and it does not contain all necessary data
        for the EUCAN-Connect Catalogue. Therefore this API is only used for
        retrieving a list with study IDs. Depending on the value of param amount
        only the total amount of studies or the real study information is returned
        @param start: study number (row) to start from, default is 0
        @param batch_size: number of studies to be returned
                           if != totalCount, a loop should be created
        """
        studies = []
        study_url = self.catalogue.catalogue_url + "/ws/studies/_rql"
        study_query = (
            "query=study(limit(" + str(start) + "," + str(batch_size) + "),fields(*))"
        )
        response = self._mica_session.post(
            study_url, headers=self._mica_headers, data=study_query
        )
        n_studies = response.json()["studyResultDto"]["totalCount"]
        if n_studies <= batch_size:
            studies_summary = response.json()["studyResultDto"][
                "obiba.mica.StudyResultDto.result"
            ]["summaries"]
        else:
            study_query = (
                "query=study(limit("
                + str(start)
                + ","
                + str(n_studies)
                + "),fields(*))"
            )
            response = self._mica_session.post(
                study_url, headers=self._mica_headers, data=study_query
            )
            studies_summary = response.json()["studyResultDto"][
                "obiba.mica.StudyResultDto.result"
            ]["summaries"]

        for study in studies_summary:
            studies.append(study["id"])

        return studies

    def get_mica_study_data(self, mica_studies: List):
        """
        Get the data of all studies from a catalogue by using the Mica REST API Client.
        It returns the complete study data.
        @param mica_studies List: a list with Mica study IDs.
        """
        study_data = []
        for study in mica_studies:

            study_url = self.catalogue.catalogue_url + "/ws/study/" + study
            response = self._mica_session.get(study_url, headers=self._mica_headers)
            study_data.append(response.json())
        return study_data

    def _convert_studies_to_eucan(self, study_data) -> CatalogueData:
        """
        Converts mica study data to the EUCAN-Catalogue format
        Fills the four EUCAN-Connect tables for the specific source catalogue

        :param study_data: the Mica study data
        :return: a CatalogueData object
        """

        tables = dict()

        for table_type in TableType.get_import_order():
            id_ = table_type.base_id
            meta = self.eucan_session.get_meta(id_)

            if id_ == "eucan_events":
                tables[table_type] = Table.of(
                    table_type=table_type,
                    meta=meta,
                    rows=self._to_events(table_type, study_data),
                )
            elif id_ == "eucan_persons":
                tables[table_type] = Table.of(
                    table_type=table_type,
                    meta=meta,
                    rows=self._to_persons(table_type, study_data),
                )
            elif id_ == "eucan_population":
                tables[table_type] = Table.of(
                    table_type=table_type,
                    meta=meta,
                    rows=self._to_populations(table_type, study_data),
                )
            elif id_ == "eucan_study":
                tables[table_type] = Table.of(
                    table_type=table_type,
                    meta=meta,
                    rows=self._to_studies(table_type, study_data),
                )

        return CatalogueData.from_dict(
            catalogue=self.catalogue, source=self.catalogue.description, tables=tables
        )

    def _to_events(self, table_type, study_data: List[dict]) -> List[dict]:
        events = []
        for study in study_data:
            study_id = study["id"]
            study_acronym = next(
                item for item in study["acronym"] if item["lang"] == "en"
            )["value"]
            for pop in study["populations"]:
                try:
                    for dce in pop["dataCollectionEvents"]:
                        event = {}
                        event["id"] = (
                            self.catalogue.get_id_prefix(table_type)
                            + pop["id"]
                            + dce["id"]
                        )
                        event["name"] = (
                            study_acronym
                            + "-"
                            + next(
                                item for item in dce["name"] if item["lang"] == "en"
                            )["value"]
                        )
                        event["source_catalogue"] = self.catalogue.code
                        try:
                            event["description"] = next(
                                item
                                for item in dce["description"]
                                if item["lang"] == "en"
                            )["value"]
                        except Exception:
                            event["description"] = None

                        start_year = dce.get("startYear")
                        end_year = dce.get("endYear")
                        start_month = dce.get("startMonth")
                        end_month = dce.get("endMonth")
                        event["start_end_year"] = str(start_year) + "-" + str(end_year)
                        event["start_end_month"] = (
                            str(start_month) + "-" + str(end_month)
                        )
                        dce_content = json.loads(dce["content"])
                        if dce_content:
                            event["datasources_type"] = []
                            event["biosamples_type"] = []
                            event["type_administrative_databases"] = []
                            if "dataSources" in dce_content.keys():
                                for source in dce_content.get("dataSources"):
                                    event["datasources_type"].append(source)
                                    if source not in self.ref_data.all_refs(
                                        "data_sources"
                                    ):
                                        self.ref_data.add_new_ref(
                                            "data_sources", source
                                        )
                                        self.printer.print(
                                            f"A new DataSource: {source} will be added "
                                            f"to the EUCAN-Connect Catalogue"
                                        )
                            if "bioSamples" in dce_content.keys():
                                for sample in dce_content.get("bioSamples"):
                                    event["biosamples_type"].append(sample)
                                    if sample not in self.ref_data.all_refs(
                                        "biosamples"
                                    ):
                                        self.ref_data.add_new_ref("biosamples", sample)
                                        self.printer.print(
                                            f"A new bioSample: {sample} will be added "
                                            f"to the EUCAN-Connect Catalogue"
                                        )
                            if "administrativeDatabases" in dce_content.keys():
                                for db_type in dce_content.get(
                                    "administrativeDatabases"
                                ):
                                    event["type_administrative_databases"].append(
                                        db_type
                                    )
                                    if db_type not in self.ref_data.all_refs(
                                        "database_types"
                                    ):
                                        self.ref_data.add_new_ref(
                                            "database_types", db_type
                                        )
                                        self.printer.print(
                                            f"A new database type: {db_type} will be "
                                            f"added to the EUCAN-Connect Catalogue"
                                        )
                            event["type_administrative_databases"] = dce_content.get(
                                "administrativeDatabases"
                            )
                        events.append(event)
                        self.studyMrefs.setdefault(study["id"], {}).setdefault(
                            "data_collection_events", []
                        ).append(event["id"])
                except KeyError as e:
                    if e.args[0] == "dataCollectionEvents":
                        warning = EucanWarning(
                            f"Study {study_id} has no data collection events"
                        )
                        self.printer.print_warning(warning)
                        self.warnings.append(warning)
                    else:
                        raise EucanError(
                            f" KeyError in function _to_events: {e.args[0]} "
                            f"does not exist"
                        )
                except Exception as e:
                    raise EucanError(f" An error occurred in function _to_events: {e}")
        return events

    def _to_persons(self, table_type, study_data: List[dict]) -> List[dict]:
        persons = []
        id_prefix = self.catalogue.get_id_prefix(table_type)
        for study in study_data:
            if "memberships" in study:
                for row in study["memberships"]:
                    for member in row["members"]:
                        if not any(
                            person["id"] == id_prefix + member["id"]
                            for person in persons
                        ):
                            person = {}
                            person["id"] = id_prefix + member["id"]
                            person["source_catalogue"] = self.catalogue.code
                            try:
                                person["title"] = member["title"]
                            except Exception:
                                pass
                            try:
                                person["first_name"] = member["firstName"]
                            except KeyError as e:
                                if e.args[0] == ["firstName"]:
                                    warning = EucanWarning(
                                        f"Person {member['id']} of study {study['id']} "
                                        f"has no first name"
                                    )
                                    self.printer.print_warning(warning)
                                    self.warnings.append(warning)
                            except Exception as e:
                                raise EucanError(
                                    f" An error occurred in function _to_persons"
                                    f" defining the first name: {e}"
                                )
                            person["last_name"] = member["lastName"]
                            person[
                                "title_after"
                            ] = None  # Not sure if this is in the Mica data
                            try:
                                person["email"] = member["email"]
                            except Exception:
                                pass
                            try:
                                person["phone"] = member["phone"]
                            except Exception:
                                pass
                            try:
                                person["address"] = next(
                                    item
                                    for item in member["institution"]["address"][
                                        "street"
                                    ]
                                    if item["lang"] == "en"
                                )["value"]
                            except Exception:
                                pass
                            try:
                                person["zip"] = member["institution"]["address"]["zip"]
                            except Exception:
                                pass
                            try:
                                person["city"] = next(
                                    item
                                    for item in member["institution"]["address"]["city"]
                                    if item["lang"] == "en"
                                )["value"]
                            except Exception:
                                pass
                            try:
                                person["country"] = self.iso_countries.get_country_id(
                                    member["institution"]["address"]["country"]["iso"]
                                )
                                if not person["country"]:
                                    country_not_found = member["institution"][
                                        "address"
                                    ]["country"]["iso"]
                                    warning = EucanWarning(
                                        f"Country "
                                        f"{country_not_found} "
                                        f"not found in the IsoCountryData"
                                    )
                                    self.printer.print_warning(warning)
                                    self.warnings.append(warning)
                            except KeyError as e:
                                if e.args[0] in ["institution", "address", "country"]:
                                    pass
                                else:
                                    raise EucanError(
                                        f" KeyError in function _to_persons defining "
                                        f"country: {e.args[0]} does not exist"
                                    )
                            except Exception as e:
                                raise EucanError(
                                    f" An error occurred in function _to_persons "
                                    f"defining country: {e}"
                                )
                            persons.append(person)
                        if row["role"] == "contact":
                            self.studyMrefs.setdefault(study["id"], {}).setdefault(
                                "contacts", []
                            ).append(id_prefix + member["id"])
                        elif row["role"] == "investigator":
                            self.studyMrefs.setdefault(study["id"], {}).setdefault(
                                "principal_investigators", []
                            ).append(id_prefix + member["id"])
                        else:
                            warning = EucanWarning(
                                f"Study {study['id']} has an unknown role {row['role']}"
                            )
                            self.printer.print_warning(warning)
                            self.warnings.append(warning)
            else:
                warning = EucanWarning(f"Study {study['id']} has no persons")
                self.printer.print_warning(warning)
                self.warnings.append(warning)
        return persons

    def _to_populations(self, table_type, study_data: List[dict]) -> List[dict]:
        populations = []
        selection_criteria = {
            "countriesIso": "country",
            "etnicOrigin": "etnic_origin",
            "gender": "sex",
            "healthStatus": "health_status",
            "newborn": "newborn",
            "otherCriteria": "other",
            "territory": "territory",
            "twins": "twin",
            "ageMin": "age",
            "ageMax": "age",
            "pregnantWoman": "pregnant_woman",
        }
        for study in study_data:
            for pop in study["populations"]:
                population = {}
                population["id"] = self.catalogue.get_id_prefix(table_type) + pop["id"]
                population["name"] = next(
                    item for item in pop["name"] if item["lang"] == "en"
                )["value"]
                population["description"] = next(
                    item for item in pop["description"] if item["lang"] == "en"
                )["value"]
                population["source_catalogue"] = self.catalogue.code
                if "content" in pop:
                    pop_content = json.loads(pop["content"])
                    if "selectionCriteria" in pop_content.keys():
                        try:
                            population["selection_criteria_supplement"] = pop_content[
                                "selectionCriteria"
                            ]["inclusion"]["index"]["en"]
                        except Exception:
                            pass
                        population["selection_criteria"] = []
                        for criterium in selection_criteria.keys():
                            try:
                                if (
                                    type(pop_content["selectionCriteria"][criterium])
                                    is list
                                ):
                                    for value in pop_content["selectionCriteria"][
                                        criterium
                                    ]:
                                        if len(value) > 0:
                                            population["selection_criteria"].append(
                                                selection_criteria[criterium]
                                            )
                                            break
                                elif (
                                    type(pop_content["selectionCriteria"][criterium])
                                    is dict
                                ):
                                    for key in pop_content["selectionCriteria"][
                                        criterium
                                    ].keys():
                                        if (
                                            len(
                                                pop_content["selectionCriteria"][
                                                    criterium
                                                ][key]
                                            )
                                            > 0
                                        ):
                                            population["selection_criteria"].append(
                                                selection_criteria[criterium]
                                            )
                                            break
                                elif (
                                    type(pop_content["selectionCriteria"][criterium])
                                    is bool
                                ):
                                    if pop_content["selectionCriteria"][criterium]:
                                        population["selection_criteria"].append(
                                            selection_criteria[criterium]
                                        )
                                # elif type (
                                #      type(pop_content["selectionCriteria"][criterium])
                                #      is str
                                #      ):
                                # if len(pop_content["selectionCriteria"][criterium])
                                # > 0:
                                # \
                                # or pop_content["selectionCriteria"][criterium]
                                # != "None":
                                # population["selection_criteria"].append(selection_criteria[criterium])
                                else:
                                    warning = EucanWarning(
                                        f" Undefined pop_content selection criteria "
                                        f"type"
                                        f"{pop_content['selectionCriteria'][criterium]}"
                                        f" found for {criterium}"
                                    )
                                    self.printer.print_warning(warning)
                                    self.warnings.append(warning)

                            except Exception:
                                pass
                    else:
                        warning = EucanWarning(
                            f"Population {population['id']} has no selection criteria"
                        )
                        self.printer.print_warning(warning)
                        self.warnings.append(warning)
                    if "recruitment" in pop_content.keys():
                        try:
                            population["recruitment_sources_supplement"] = pop_content[
                                "recruitment"
                            ]["info"]["en"]
                        except Exception:
                            pass
                        try:
                            if (
                                "recruitment_sources_supplement"
                                not in population.keys()
                            ):
                                population["recruitment_sources_supplement"] = ""
                            for method in pop_content["recruitment"][
                                "recruitmentMethod"
                            ]:
                                population["recruitment_sources_supplement"] = (
                                    population["recruitment_sources_supplement"]
                                    + method
                                )
                        except Exception:
                            pass
                        try:
                            population["recruitment_sources"] = []
                            for source in pop_content["recruitment"]["dataSources"]:
                                population["recruitment_sources"].append(source)
                                if source not in self.ref_data.all_refs(
                                    "recruitment_sources"
                                ):
                                    self.ref_data.add_new_ref(
                                        "recruitment_sources", source
                                    )
                                    self.printer.print(
                                        f"A new recruitment source {source} will be "
                                        f"added to the EUCAN-Connect Catalogue"
                                    )
                        except KeyError as e:
                            if e.args[0] != "dataSources":
                                raise EucanError(
                                    f" An unexpected KeyError occurred in "
                                    f"function _to_populations: {e}"
                                )
                        except Exception as e:
                            raise EucanError(
                                f" An error occurred in function _to_populations: {e}"
                            )
                    else:
                        warning = EucanWarning(
                            f"Population {population['id']} has no recruitment sources"
                        )
                        self.printer.print_warning(warning)
                        self.warnings.append(warning)
                    if "numberOfParticipants" in pop_content.keys():
                        try:
                            population["number_of_participants"] = pop_content[
                                "numberOfParticipants"
                            ]["participant"]["number"]
                        except Exception as e:
                            if str(e) != "number":
                                raise EucanError(
                                    f" An error occurred in defining "
                                    f"number_of_participants in function "
                                    f"_to_populations: {e}"
                                )
                        try:
                            population["participants_with_biosamples"] = pop_content[
                                "numberOfParticipants"
                            ]["sample"]["number"]
                        except Exception as e:
                            if str(e) != "'number'":
                                raise EucanError(
                                    f" An error occurred in defining "
                                    f"participant_with_biosamples in "
                                    f"function _to_populations: {e}"
                                )
                        try:
                            population[
                                "number_of_participants_supplement"
                            ] = pop_content["numberOfParticipants"]["info"]["en"]
                        except Exception as e:
                            if str(e) != "'info'":
                                raise EucanError(
                                    f" An error occurred in defining "
                                    f"number_of_participants_supplement in "
                                    f"function _to_populations: {e}"
                                )
                    else:
                        warning = EucanWarning(
                            f"Population {population['id']} has no number "
                            f"of participants information"
                        )
                        self.printer.print_warning(warning)
                        self.warnings.append(warning)
                else:
                    warning = EucanWarning(
                        f"Population {population['id']} has no "
                        f"selection criteria and recruitment sources"
                    )
                    self.printer.print_warning(warning)
                    self.warnings.append(warning)

                populations.append(population)
                self.studyMrefs.setdefault(study["id"], {}).setdefault(
                    "populations", []
                ).append(population["id"])
        return populations

    def _to_studies(self, table_type, study_data: List[dict]) -> List[dict]:
        studies = []
        id_prefix = self.catalogue.get_id_prefix(table_type)
        for row in study_data:
            study = {}
            study["id"] = id_prefix + row["id"]
            study["study_name"] = next(
                item for item in row["name"] if item["lang"] == "en"
            )["value"]
            study["acronym"] = next(
                item for item in row["acronym"] if item["lang"] == "en"
            )["value"]
            study["objectives"] = next(
                item for item in row["objectives"] if item["lang"] == "en"
            )["value"]
            study["source_catalogue"] = self.catalogue.code
            if "content" in row.keys():
                study_content = json.loads(row["content"])
                try:
                    study["start_year"] = study_content["startYear"]
                except Exception:
                    pass
                try:
                    study["end_year"] = study_content["endYear"]
                except Exception:
                    pass
                try:
                    study["marker_paper"] = study_content["markerPaper"]
                except Exception:
                    pass
                try:
                    study["website"] = study_content["website"]
                except Exception:
                    pass
                try:
                    study["funding"] = study_content["funding"]["en"]
                except Exception:
                    pass
                # TODO: access_possiblity and contact_procedures need to be defined
                # TODO: in another way in the EUCAN-Connect data model in my opinion
                # TODO: Recap has a list with categories which can be accessed
                # TODO: Maelstrom has a variable yes/no for data, biosamples etc
                # TODO: access contact person and procedure are not available in RECAP
                # TODO: Maelstrom does has this info, but then it should
                # TODO: become a concatenated string (?)
                # if type(study_content["access"]) is list:
                #     # RECAP
                #     for category in study_content["access"]:
                #         if category in ["data", "bio_samples"]:
                #             study["access_possibility"] = True
                #             break
                #         if category in ["other"]:
                #            study["access_possibility"] = False
                # elif type(study_content["access"]) is dict:
                # study["contact_procedures"] =
                # TODO: study_timeline is not an attribute in Mica
                #  (it's a graph based on data collection events data)
                # study["study_timeline"] =
                if "methods" in study_content.keys():
                    study["study_design"] = study_content["methods"]["design"]
                    if "followUpInfo" in study_content["methods"]:
                        study["follow_up_info"] = study_content["methods"][
                            "followUpInfo"
                        ]["en"]
                    else:
                        try:
                            study["follow_up_info"] = (
                                "number of follow-ups is"
                                + study_content["methods"]["numberOfFollowUps"]
                            )
                        except Exception:
                            pass
                    try:
                        study["recruitment_target"] = ""
                        for target in study_content["methods"]["recruitments"]:
                            study["recruitment_target"] = (
                                study["recruitment_target"] + target + " and "
                            )
                    except Exception:
                        pass
                    if study["recruitment_target"]:
                        study["recruitment_target"] = study["recruitment_target"][
                            : len(study["recruitment_target"]) - 5
                        ]
                else:
                    warning = EucanWarning(
                        f"Study {study['id']} has no 'methods' information"
                    )
                    self.printer.print_warning(warning)
                    self.warnings.append(warning)
                if "numberOfParticipants" in study_content.keys():
                    try:
                        study["number_of_participants"] = study_content[
                            "numberOfParticipants"
                        ]["participant"]["number"]
                    except Exception:
                        pass
                    try:
                        study["participants_with_biosamples"] = study_content[
                            "numberOfParticipants"
                        ]["sample"]["number"]
                    except Exception:
                        pass
                    try:
                        study["number_of_participants_supplement"] = study_content[
                            "numberOfParticipants"
                        ]["info"]["en"]
                    except Exception:
                        pass
                else:
                    warning = EucanWarning(
                        f"Study {study['id']} has no numberOfParticipants information"
                    )
                    self.printer.print_warning(warning)
                    self.warnings.append(warning)
            else:
                warning = EucanWarning(
                    f"Study {study['id']} has no 'content' information"
                )
                self.printer.print_warning(warning)
                self.warnings.append(warning)

            studies.append(study)
        return studies
