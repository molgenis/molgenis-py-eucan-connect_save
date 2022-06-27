"""
 202011 Dieuwke Roelofs-Prins
 Script to import birthcohort data (using JSON API from birthcohorts.net)
 and add the data to the EUCAN-Connect Catalogue.
"""

# Import module(s)
import argparse
import logging as log
import re

import molgenis.client as molgenis
import requests


# Define function(s)
# Function to derive the e-mail address from a list
def derive_email_address(email_list):
    if i <= len(re.split(", |; ", email_list)):
        email_address = re.split(", |; ", email_list)[i - 1].strip()
    else:
        email_address = re.split(", |; ", email_list)[
            len(re.split(", |; ", email_list)) - 1
        ].strip()
    if "@" not in email_address:
        if "at" in email_address:
            email_address = email_address.replace("at", "@").replace(" ", "")
        else:
            log.warning("Email address " + email_address + " is not valid")
            email_address = None
    else:
        email_address = None

    return email_address


# Function to derive from the birth cohort contact name the titles, first and last name
def derive_first_last_name(full_name, known_titles):
    derived_first_name = ""
    derived_last_name = ""
    second_first_names = [
        "Aysimi",
        "Cristina",
        "Eek",
        "Kristine",
        "L",
        "M",
        "Marie",
        "Mette",
        "Peter",
        "Pia",
    ]
    derived_titles = []
    if len(full_name) > 0:
        for known_title in known_titles:
            if known_title in full_name:
                derived_titles.append(known_title)
                full_name = full_name.replace(known_title, "")
        full_name = full_name.replace(",", "")
        full_name = full_name.strip()
        if full_name == "":
            derived_first_name = "Unknown"
            derived_last_name = "Unknown"
        else:
            if len(full_name.split(" ")) > 1:
                if full_name.split(" ")[1] in second_first_names:
                    derived_first_name = (
                        full_name.split(" ")[0] + " " + full_name.split(" ")[1]
                    )
                else:
                    derived_first_name = full_name.split(" ")[0]
                derived_last_name = full_name.replace(derived_first_name, "")
                # Check for abbreviations
                while derived_last_name.find(".") != -1:
                    if derived_first_name != "":
                        derived_first_name = (
                            derived_first_name
                            + " "
                            + derived_last_name[
                                derived_last_name.find(".")
                                - 1 : derived_last_name.find(".")
                                + 1
                            ]
                        )
                    else:
                        derived_first_name = derived_last_name[
                            derived_last_name.find(".")
                            - 1 : derived_last_name.find(".")
                            + 1
                        ]
                    derived_last_name = derived_last_name.replace(
                        derived_last_name[
                            derived_last_name.find(".")
                            - 1 : derived_last_name.find(".")
                            + 1
                        ],
                        "",
                    )
                # Check for parenthesis
                if "(" in derived_last_name:
                    if derived_first_name != "":
                        derived_first_name = (
                            derived_first_name
                            + " "
                            + derived_last_name[
                                derived_last_name.find("(") : derived_last_name.find(
                                    ")"
                                )
                                + 1
                            ]
                        )
                    else:
                        derived_first_name = derived_last_name[
                            derived_last_name.find("(") : derived_last_name.find(")")
                            + 1
                        ]
                    derived_last_name = derived_last_name.replace(
                        derived_last_name[
                            derived_last_name.find("(") : derived_last_name.find(")")
                            + 1
                        ],
                        "",
                    )
            else:
                derived_first_name = "Unknown"
                derived_last_name = full_name.strip()
                if derived_last_name.find("."):
                    # Check for abbreviations
                    while derived_last_name.find(".") != -1:
                        if derived_first_name != "Unknown":
                            derived_first_name = (
                                derived_first_name
                                + " "
                                + derived_last_name[
                                    derived_last_name.find(".")
                                    - 1 : derived_last_name.find(".")
                                    + 1
                                ]
                            )
                        else:
                            derived_first_name = derived_last_name[
                                derived_last_name.find(".")
                                - 1 : derived_last_name.find(".")
                                + 1
                            ]
                        derived_last_name = derived_last_name.replace(
                            derived_last_name[
                                derived_last_name.find(".")
                                - 1 : derived_last_name.find(".")
                                + 1
                            ],
                            "",
                        )
    return derived_titles, derived_first_name.strip(), derived_last_name.strip()


parser = argparse.ArgumentParser()
parser.add_argument(
    "-u",
    "--username",
    dest="username",
    default="admin",
    help="Username for the MOLGENIS server",
)
parser.add_argument(
    "-p", "--password", dest="password", help="Password to login to the MOLGENIS server"
)
parser.add_argument(
    "-s",
    "--server",
    dest="server",
    default="https://catalogue.eucanconnect.eu/api/",
    help="URL of the API end-point of the MOLGENIS server.",
)
parser.add_argument(
    "-l",
    "--log",
    dest="log_level",
    nargs="?",
    const="INFO",
    default="INFO",
    help="Provide the log level, levels are CRITICAL, ERROR, WARNING, INFO and DEBUG",
)

args = parser.parse_args()

if args.username is None or args.password is None or args.server is None:
    log.error("Must specify username, password and server.")
    raise SystemExit("Must specify username, password and server.")

# Define variable(s)
log.getLogger().setLevel(args.log_level.upper())

# Constant variables
birthCohortsUrl = "www.birthcohorts.net/wp-content/themes/x-child/rss.cohorts.php?"
call_limit = 10
nRetrievedCohorts = 0
user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
header = {"User-Agent": user_agent}

# Create the molgenis session and login
eucan_session = molgenis.Session(args.server)
eucan_session.login(args.username, args.password)

# Dictionaries
countryIDs = {}
existing_events = {}
existing_persons = {}
existing_populations = {}
existing_studies = {}

# List variables
cohort_keys = ["identification", "description", "questionnaire", "comments"]
eucan_events = []
eucan_persons = []
eucan_populations = []
eucan_studies = []
eucan_delete_events = []
eucan_delete_persons = []
eucan_delete_populations = []
eucan_delete_studies = []
existing_event_ids = []
existing_person_ids = []
existing_population_ids = []
existing_study_ids = []
title_list = [
    "Adj.",
    "prof.",
    "Prof.",
    "Professor.",
    "Professor",
    "Prof",
    "Dr.",
    "dr.",
    "Dr",
    "PharmD",
    "PhD",
    "Associate",
    "MD",
    "Executive",
    "MSc",
    "Clinical study nurse",
]

# Get the total number of cohorts from birthcohorts.net ######
BirthCohorts = requests.get(
    url="http://" + birthCohortsUrl + "limit=0&json", headers=header
)

if len(BirthCohorts.json()) == 0:
    raise SystemExit("No data from birthcohorts.net found?!?")

ApiCount = BirthCohorts.json()["@attributes"]["count"]
log.info("API call count to www.birthcohorts.net is %s", ApiCount)

# Get the list with countries from EUCAN
countries = eucan_session.get("eucan_country", batch_size=1000)

if len(countries) == 0:
    raise SystemExit("No countries found?!?")

for country in countries:
    countryIDs[country["label"]] = country["id"]

# Get the current list with persons/contacts from EUCAN
persons = eucan_session.get(
    "eucan_persons", q='id=like="birthcohorts"', batch_size=1000
)

if len(persons) == 0:
    # raise SystemExit('No persons found?!?')
    # log.error('No persons found in EUCAN')
    log.warning("No birth cohort persons found in EUCAN")

for person in persons:
    existing_person_ids.append(person["id"])
    existing_persons[person["id"]] = person

# Get the current list of birth cohort studies from EUCAN
studies = eucan_session.get(
    "eucan_study",
    q='id=like="birthcohorts"',
    batch_size=1000,
)

if len(studies) == 0:
    log.warning("No birth cohort studies found in EUCAN")
else:
    log.info("Number of current birth cohort studies in EUCAN is %s", len(studies))

for study in studies:
    existing_study_ids.append(study["id"])
    existing_studies[study["id"]] = study

# Get the current list of events from EUCAN
events = eucan_session.get("eucan_events", q='id=like="birthcohorts"', batch_size=1000)

if len(events) == 0:
    # raise SystemExit('No data events found?!?')
    # log.error('No Data Events found in EUCAN')
    log.warning("No birth cohort Data Events found in EUCAN")

for event in events:
    existing_event_ids.append(event["id"])
    existing_events[event["id"]] = event

# Get the current list of populations from EUCAN
populations = eucan_session.get(
    "eucan_population", attributes="id", q='id=like="birthcohorts"', batch_size=1000
)

if len(populations) == 0:
    log.warning("No birth cohort populations found in EUCAN")

for population in populations:
    existing_population_ids.append(population["id"])
    existing_populations[population["id"]] = population

# Get the birth cohorts data from birthcohorts.net ##
pageNr = 1
while pageNr < int(ApiCount) + call_limit:
    BirthCohorts = requests.get(
        url="http://"
        + birthCohortsUrl
        + "limit="
        + str(call_limit)
        + "&page="
        + str(pageNr)
        + "&json",
        headers=header,
    )
    log.debug(BirthCohorts.json()["@attributes"])
    pageNr = pageNr + call_limit

    for key in BirthCohorts.json().keys():
        if key not in ["@attributes", "cohort"]:
            log.error(
                "There is another dictionary key ("
                + key
                + ") found than cohort and @attributes!"
            )
            raise SystemExit(
                "There is another dictionary key ("
                + key
                + ") found than cohort and @attributes!"
            )

    if "cohort" not in BirthCohorts.json().keys():
        log.debug("All birth cohorts are retrieved")
        break  # All cohorts are retrieved add/update the data

    for birth_cohort in BirthCohorts.json()["cohort"]:
        log.debug(
            birth_cohort["identification"]["id"], birth_cohort["identification"]["name"]
        )

    for birth_cohort in BirthCohorts.json()["cohort"]:
        nRetrievedCohorts += 1
        # Empty the variables
        contactIDs = []
        eucan_contacts = []
        eucan_event = {}
        eucan_pop_children = {}
        eucan_pop_mothers = {}
        eucan_pop_fathers = {}
        eucan_pop_grandparents = {}
        eucan_pop_fam_members = {}
        eucan_study = {}
        first_name = None
        investigatorIDs = []
        last_name = None
        study_acronym = ""
        study_id = None
        titles = []

        for key in birth_cohort.keys():
            if key not in cohort_keys:
                raise SystemExit(key + " is a new item in the birth_cohort dictionary!")
            if key == "identification":
                # Derivation/Definition of some variables (and missing values)
                if (
                    type(birth_cohort["identification"]["contact"]["name"]) is dict
                    and len(birth_cohort["identification"]["contact"]["name"]) == 0
                ):
                    contacts = "Unknown"
                else:
                    contacts = birth_cohort["identification"]["contact"]["name"]
                if (
                    type(birth_cohort["identification"]["investigator"]["name"]) is dict
                    and len(birth_cohort["identification"]["investigator"]["name"]) == 0
                ):
                    investigators = "Unknown"
                else:
                    investigators = birth_cohort["identification"]["investigator"][
                        "name"
                    ]
                if (
                    type(birth_cohort["identification"]["contact"]["email"]) is dict
                    and len(birth_cohort["identification"]["contact"]["email"]) == 0
                ):
                    contact_emails = None
                else:
                    contact_emails = birth_cohort["identification"]["contact"]["email"]
                if (
                    type(birth_cohort["identification"]["investigator"]["email"])
                    is dict
                    and len(birth_cohort["identification"]["investigator"]["email"])
                    == 0
                ):
                    investigator_emails = None
                else:
                    investigator_emails = birth_cohort["identification"][
                        "investigator"
                    ]["email"]
                if type(birth_cohort["identification"]["website"]) is not dict:
                    if len(birth_cohort["identification"]["website"].split()) > 1:
                        log.warning(
                            "More than one website available for study "
                            + birth_cohort["identification"]["name"]
                            + " "
                            + birth_cohort["identification"]["website"]
                        )
                        # If more than one website is available, the first one is stored
                        website = (
                            birth_cohort["identification"]["website"].split()[0].strip()
                        )
                    else:
                        website = birth_cohort["identification"]["website"].strip()
                else:
                    website = None

                # Get the right country ID
                country = None
                if (
                    type(birth_cohort["identification"]["country"]) is dict
                    and len(birth_cohort["identification"]["country"]) == 0
                ):
                    country = None
                else:
                    for country_name in countryIDs.keys():
                        if country_name in birth_cohort["identification"]["country"]:
                            country = countryIDs[country_name]
                            break
                    if country is None:
                        country = birth_cohort["identification"]["country"]

                # Define the study_id
                study_id = "birthcohorts:" + birth_cohort["identification"]["id"]

                if (
                    type(birth_cohort["identification"]["abbreviation"]) is dict
                    and len(birth_cohort["identification"]["abbreviation"]) == 0
                ):
                    if len(birth_cohort["identification"]["name"].split()) == 1:
                        study_acronym = birth_cohort["identification"]["name"]
                        log.debug(
                            "Birth cohort acronym: one word",
                            birth_cohort["identification"]["name"],
                            study_acronym,
                        )
                    else:
                        for word in birth_cohort["identification"]["name"].split():
                            study_acronym = study_acronym + word[0].upper()
                        log.debug(
                            "Birth cohort acronym: more than one word",
                            birth_cohort["identification"]["name"],
                            study_acronym,
                        )
                else:
                    study_acronym = birth_cohort["identification"][
                        "abbreviation"
                    ].replace(",", " and")
                    log.debug(
                        "Existing acronym",
                        birth_cohort["identification"]["name"],
                        study_acronym,
                    )

                # Contacts
                if contacts != "Unknown":
                    split_list = ""
                    for title in title_list:
                        if ", " + title in contacts:
                            split_list = ";|/|&| and "
                    if split_list == "":
                        if "," and ";" in contacts:
                            split_list = ";|/|&| and "
                        else:
                            split_list = ",|;|/|&| and "

                    for i, contact_name in enumerate(
                        re.split(split_list, contacts), start=1
                    ):
                        contact_name = contact_name.strip()
                        if len(contact_name) > 0:
                            titles, first_name, last_name = derive_first_last_name(
                                contact_name, title_list
                            )
                            log.debug(
                                "study_id",
                                "\t",
                                study_id,
                                "\t",
                                "contacts",
                                "\t",
                                contacts,
                                "\t",
                                "contact_name",
                                "\t",
                                contact_name,
                                "\t",
                                "titles",
                                "\t",
                                titles,
                                "\t",
                                "first_name",
                                "\t",
                                first_name,
                                "\t",
                                "last_name",
                                "\t",
                                last_name,
                            )
                        if first_name != "Unknown" or last_name != "Unknown":

                            contact_id = (
                                study_id + ":contactID:" + last_name + first_name
                            )
                            contact_id = (
                                contact_id.replace(" ", "")
                                .replace("-", "")
                                .replace(".", "")
                                .replace("(", "")
                                .replace(")", "")
                                .replace("ć", "c")
                                .replace("á", "a")
                                .replace("í", "i")
                                .replace("ñ", "n")
                                .replace("ä", "a")
                                .replace("ø", "o")
                                .replace("è", "e")
                                .replace("ö", "o")
                                .replace("é", "e")
                                .replace("ã", "a")
                                .replace("Ö", "O")
                                .replace("š", "s")
                                .replace("Å", "A")
                            )  # random_string(n_chars=10)
                            if contact_emails is not None:
                                contact_email = derive_email_address(contact_emails)
                            else:
                                contact_email = None
                            if contact_id not in contactIDs:
                                contactIDs.append(contact_id)
                                eucan_contacts.append(
                                    {
                                        "id": contact_id,
                                        "title": " ".join(titles),
                                        "country": country,
                                        "first_name": first_name,
                                        "last_name": last_name,
                                        "email": contact_email,
                                    }
                                )

                # Investigators
                split_list = ""
                for title in title_list:
                    if ", " + title in investigators:
                        split_list = ";|/|&| and "
                if split_list == "":
                    if "," and ";" in investigators:
                        split_list = ";|/|&| and "
                    else:
                        split_list = ",|;|/|&| and "
                for i, contact_name in enumerate(
                    re.split(split_list, investigators), start=1
                ):
                    contact_name = (
                        contact_name.replace("co-PI:", "").replace("PI:", "").strip()
                    )
                    if len(contact_name) > 0:
                        titles, first_name, last_name = derive_first_last_name(
                            contact_name, title_list
                        )
                        log.debug(
                            "study_id",
                            "\t",
                            study_id,
                            "\t",
                            "investigators",
                            "\t",
                            investigators,
                            "\t",
                            "contact_name",
                            "\t",
                            contact_name,
                            "\t",
                            "titles",
                            "\t",
                            titles,
                            "\t",
                            "first_name",
                            "\t",
                            first_name,
                            "\t",
                            "last_name",
                            "\t",
                            last_name,
                        )
                        if first_name != "Unknown" or last_name != "Unknown":
                            contact_id = (
                                study_id + ":contactID:" + last_name + first_name
                            )
                            contact_id = (
                                contact_id.replace(" ", "")
                                .replace("-", "")
                                .replace("(", "")
                                .replace(")", "")
                                .replace(".", "")
                                .replace("ć", "c")
                                .replace("á", "a")
                                .replace("í", "i")
                                .replace("ñ", "n")
                                .replace("ä", "a")
                                .replace("ø", "o")
                                .replace("è", "e")
                                .replace("ö", "o")
                                .replace("é", "e")
                                .replace("ã", "a")
                                .replace("Ö", "O")
                                .replace("š", "s")
                                .replace("Å", "A")
                            )  # random_string(n_chars=10)
                            contact_email = None
                            if investigator_emails is not None:
                                contact_email = derive_email_address(
                                    investigator_emails
                                )
                            if contact_id not in investigatorIDs:
                                investigatorIDs.append(contact_id)
                            # Check if the person is already in eucan_contacts otherwise add it
                            if (
                                len(
                                    [
                                        contact
                                        for contact in eucan_contacts
                                        if contact["id"] == contact_id
                                    ]
                                )
                                == 0
                            ):
                                eucan_contacts.append(
                                    {
                                        "id": contact_id,
                                        "title": " ".join(titles),
                                        "country": country,
                                        "first_name": first_name,
                                        "last_name": last_name,
                                        "email": contact_email,
                                    }
                                )
                            else:
                                # Check if any new information is available that can be added
                                existing = [
                                    [i, d]
                                    for i, d in enumerate(eucan_contacts)
                                    if contact_id in d.values()
                                ]
                                if len(titles) > 0 and eucan_contacts[existing[0][0]][
                                    "title"
                                ] != " ".join(titles):
                                    log.debug(
                                        "new titles available:",
                                        contact_id,
                                        eucan_contacts[existing[0][0]]["title"],
                                        titles,
                                    )
                                    eucan_contacts[existing[0][0]][
                                        "title"
                                    ] = eucan_contacts[existing[0][0]][
                                        "title"
                                    ] + " ".join(
                                        titles
                                    )
                                    log.debug(
                                        "new title",
                                        eucan_contacts[existing[0][0]]["title"],
                                    )
                                if (
                                    first_name != "Unknown"
                                    and eucan_contacts[existing[0][0]]["first_name"]
                                    != first_name
                                ):
                                    log.debug(
                                        "new first name info available:",
                                        contact_id,
                                        eucan_contacts[existing[0][0]]["first_name"],
                                        first_name,
                                    )
                                    eucan_contacts[existing[0][0]]["first_name"] = (
                                        eucan_contacts[existing[0][0]]["first_name"]
                                        + first_name
                                    )
                                    log.debug(
                                        "new first_name",
                                        eucan_contacts[existing[0][0]]["first_name"],
                                    )
                                if (
                                    last_name != "Unknown"
                                    and eucan_contacts[existing[0][0]]["last_name"]
                                    != last_name
                                ):
                                    log.debug(
                                        "new last name info available:",
                                        contact_id,
                                        eucan_contacts[existing[0][0]]["last_name"],
                                        last_name,
                                    )
                                    eucan_contacts[existing[0][0]]["last_name"] = (
                                        eucan_contacts[existing[0][0]]["last_name"]
                                        + last_name
                                    )
                                    log.debug(
                                        "new last name",
                                        eucan_contacts[existing[0][0]]["last_name"],
                                    )
                                if (
                                    contact_email is not None
                                    and eucan_contacts[existing[0][0]]["contact_email"]
                                    != contact_email
                                ):
                                    log.debug(
                                        "new contact_email info available:",
                                        contact_id,
                                        eucan_contacts[existing[0][0]]["email"],
                                        contact_email,
                                    )
                                    eucan_contacts[existing[0][0]][
                                        "email"
                                    ] = contact_email
                                    log.debug(
                                        "new contact_email",
                                        eucan_contacts[existing[0][0]]["mail"],
                                    )

                eucan_study = {
                    "id": study_id,
                    "study_name": birth_cohort["identification"]["name"].strip(),
                    "acronym": study_acronym,
                    "start_year": birth_cohort["identification"]["date"][0:4].replace(
                        "0000", ""
                    ),
                    "website": website,
                    "principle_investigators": investigatorIDs,
                    "contacts": contactIDs,
                    "source_catalogue": "https://www.birthcohorts.net/birthcohorts/birthcohort/?id="
                    + birth_cohort["identification"]["id"],
                    "populations": [],
                }

            elif key == "description":
                if len(birth_cohort["description"]["enrollment"]["followup"]) > 0:
                    name = birth_cohort["description"]["enrollment"][
                        "followup"
                    ].replace("\t", "")
                    name = name.replace("\n", "")
                    name = name.replace("/", "_")
                    name = name.replace("-", "_")
                    name = name.replace(" ", "")
                    name = name.replace(".", "")
                    if name.find("(") > -1:
                        name = name[: name.find("(")]
                    name = name.strip()

                    # Definition of some variables (and missing values)
                    if (
                        type(
                            birth_cohort["description"]["enrollment"]["period"]["start"]
                        )
                        is not dict
                        and len(
                            birth_cohort["description"]["enrollment"]["period"]["start"]
                        )
                        != 0
                        and type(
                            birth_cohort["description"]["enrollment"]["period"]["end"]
                        )
                        is not dict
                        and len(
                            birth_cohort["description"]["enrollment"]["period"]["end"]
                        )
                        != 0
                    ):
                        start_end_year = (
                            birth_cohort["description"]["enrollment"]["period"][
                                "start"
                            ][0:4]
                            + "-"
                            + birth_cohort["description"]["enrollment"]["period"][
                                "end"
                            ][0:4]
                        )
                        start_end_month = (
                            birth_cohort["description"]["enrollment"]["period"][
                                "start"
                            ][5:7]
                            + "-"
                            + birth_cohort["description"]["enrollment"]["period"][
                                "end"
                            ][5:7]
                        )
                    else:
                        start_end_year = None
                        start_end_month = None

                    event_id = study_id + ":eventID:" + name  # random_string(8)
                    eucan_event = {
                        "id": event_id,
                        "name": study_acronym
                        + " - "
                        + birth_cohort["description"]["enrollment"]["followup"]
                        .replace("\t", "")
                        .strip(),
                        "description": birth_cohort["description"]["enrollment"][
                            "followup"
                        ],
                        "start_end_year": start_end_year,
                        "start_end_month": start_end_month,
                    }

                    eucan_study["data_collection_events"] = event_id

                # Population information
                if (
                    type(
                        birth_cohort["description"]["enrollment"]["criteria_exclusion"]
                    )
                    is dict
                ):
                    selection_criteria_supplement = None
                else:
                    selection_criteria_supplement = birth_cohort["description"][
                        "enrollment"
                    ]["criteria_exclusion"]
                    selection_criteria_supplement = (
                        selection_criteria_supplement.replace("\n", "")
                    )

                if int(birth_cohort["description"]["recruited"]["children"]) > 0:
                    eucan_pop_children = {
                        "id": study_id + ":populationID:children",  # +random_string(8),
                        "name": study_acronym + " - Children",
                        "number_of_participants": birth_cohort["description"][
                            "recruited"
                        ]["children"],
                        "selection_criteria_supplement": selection_criteria_supplement,
                    }
                    eucan_study["populations"].append(eucan_pop_children["id"])
                if int(birth_cohort["description"]["recruited"]["mothers"]) > 0:
                    eucan_pop_mothers = {
                        "id": study_id + ":populationID:mothers",  # +random_string(8)
                        "name": study_acronym + " - Mothers",
                        "number_of_participants": birth_cohort["description"][
                            "recruited"
                        ]["mothers"],
                        "selection_criteria_supplement": selection_criteria_supplement,
                    }
                    eucan_study["populations"].append(eucan_pop_mothers["id"])
                if int(birth_cohort["description"]["recruited"]["fathers"]) > 0:
                    eucan_pop_fathers = {
                        "id": study_id + ":populationID:fathers",  # +random_string(8)
                        "name": study_acronym + " - Fathers",
                        "number_of_participants": birth_cohort["description"][
                            "recruited"
                        ]["fathers"],
                        "selection_criteria_supplement": selection_criteria_supplement,
                    }
                    eucan_study["populations"].append(eucan_pop_fathers["id"])
                if int(birth_cohort["description"]["recruited"]["grandparents"]) > 0:
                    eucan_pop_grandparents = {
                        "id": study_id
                        + ":populationID:grandparents",  # +random_string(8)
                        "name": study_acronym + " - Grandparents",
                        "number_of_participants": birth_cohort["description"][
                            "recruited"
                        ]["grandparents"],
                        "selection_criteria_supplement": selection_criteria_supplement,
                    }
                    eucan_study["populations"].append(eucan_pop_grandparents["id"])
                if int(birth_cohort["description"]["recruited"]["familymembers"]) > 0:
                    eucan_pop_fam_members = {
                        "id": study_id
                        + ":populationID:familymembers",  # +random_string(8)
                        "name": study_acronym + " - FamilyMembers",
                        "number_of_participants": birth_cohort["description"][
                            "recruited"
                        ]["familymembers"],
                        "selection_criteria_supplement": selection_criteria_supplement,
                    }
                    eucan_study["populations"].append(eucan_pop_fam_members["id"])
                if (
                    type(birth_cohort["description"]["aim"]) is not dict
                    and len(birth_cohort["description"]["aim"]) > 0
                ):
                    eucan_study["objectives"] = birth_cohort["description"]["aim"]

        # Add the data to the lists to be uploaded
        if len(eucan_event) > 0:
            eucan_events.append(eucan_event)
        if len(eucan_contacts) > 0:
            for person in eucan_contacts:
                eucan_persons.append(person)
        if len(eucan_pop_children) > 0:
            eucan_populations.append(eucan_pop_children)
        if len(eucan_pop_mothers) > 0:
            eucan_populations.append(eucan_pop_mothers)
        if len(eucan_pop_fathers) > 0:
            eucan_populations.append(eucan_pop_fathers)
        if len(eucan_pop_grandparents) > 0:
            eucan_populations.append(eucan_pop_grandparents)
        if len(eucan_pop_fam_members) > 0:
            eucan_populations.append(eucan_pop_fam_members)
        if len(eucan_study) > 0:
            eucan_studies.append(eucan_study)

for pop in eucan_populations:
    if "name" not in pop:
        log.warning("Missing name", pop)

if len(eucan_studies) != nRetrievedCohorts:
    log.error(
        "Number of new birth cohorts and birth cohorts to be updated (",
        len(eucan_studies),
        ") is not equal to total number of birth cohorts (",
        nRetrievedCohorts,
        ")",
    )
    raise SystemExit(
        "Number of new birth cohorts and birth cohorts to be updated ("
        + str(len(eucan_studies))
        + ") is not equal to total number of birth cohorts ("
        + str(nRetrievedCohorts)
        + ")"
    )
else:
    log.info("Number of new birth cohort studies is %s", nRetrievedCohorts)


# Report the deleted birth cohort records
log.info("Report any deleted birth cohort records")
eucan_study_ids = [value for study in eucan_studies for value in study.values()]
deleted = 0
for study_id in existing_studies.keys():
    if study_id not in eucan_study_ids:
        deleted += 1
        print(
            "This birth cohort study does not exist anymore:",
            existing_studies[study_id],
        )
if deleted == 0:
    log.info("No birth cohort studies that are deleted")

eucan_person_ids = [value for person in eucan_persons for value in person.values()]
deleted = 0
for person_id in existing_persons.keys():
    if person_id not in eucan_person_ids:
        deleted += 1
        print("This birth cohort person will be deleted:", existing_persons[person_id])
if deleted == 0:
    log.info("No birth cohort persons that are deleted")

eucan_event_ids = [value for event in eucan_events for value in event.values()]
deleted = 0
for event_id in existing_events.keys():
    if event_id not in eucan_event_ids:
        deleted += 1
        print(
            "This birth cohort data collection event does not exist anymore:",
            existing_events[event_id],
        )
if deleted == 0:
    log.info("No birth cohort data collection events that are deleted")

eucan_population_ids = [
    value for population in eucan_populations for value in population.values()
]
deleted = 0
for population_id in existing_populations.keys():
    if population_id not in eucan_population_ids:
        deleted += 1
        print(
            "This birth cohort population does not exist anymore:",
            existing_populations[population_id],
        )
if deleted == 0:
    log.info("No birth cohort populations that are deleted")

# Report any new birth cohort studies, persons, data collection events and populations
log.info("Report any new birth cohort studies")
new = 0
for study in eucan_studies:
    if study["id"] not in existing_study_ids:
        new += 1
        print("New birth cohort study:", study)
if new == 0:
    log.info("No new birth cohort studies")

new = 0
for person in eucan_persons:
    if person["id"] not in existing_person_ids:
        new += 1
        print("New birth cohort person:", person)
if new == 0:
    log.info("No new birth cohort persons")

new = 0
for event in eucan_events:
    if event["id"] not in existing_event_ids:
        new += 1
        print("New birth cohort data collection event:", event)
if new == 0:
    log.info("No new birth cohort data collection events")

new = 0
for population in eucan_populations:
    if population["id"] not in existing_population_ids:
        new += 1
        print("New birth cohort population:", population)
if new == 0:
    log.info("No new birth cohort populations")

# Delete all existing birth cohort data from EUCAN
if len(eucan_studies) > 0 and len(existing_study_ids) > 0:
    log.debug("\nDelete existing birth cohort studies %s", len(existing_study_ids))
    eucan_session.delete_list("eucan_study", existing_study_ids)

if len(eucan_populations) > 0 and len(existing_population_ids) > 0:
    log.debug(
        "\nDelete existing birth cohort populations %s", len(existing_population_ids)
    )
    eucan_session.delete_list("eucan_population", existing_population_ids)

if len(eucan_events) > 0 and len(existing_event_ids) > 0:
    log.debug("\nDelete existing birth cohort events %s", len(existing_event_ids))
    eucan_session.delete_list("eucan_events", existing_event_ids)

if len(eucan_persons) > 0 and len(existing_person_ids) > 0:
    log.debug("\nDelete existing birth cohort persons %s", len(existing_person_ids))
    eucan_session.delete_list("eucan_persons", existing_person_ids)

# Add new birth cohorts to EUCAN
if len(eucan_persons) > 0:
    log.debug("\nAdd %s Contacts", len(eucan_persons))
    for person in eucan_persons:
        if len(person["first_name"]) == 0:
            print(person)
    eucan_session.add_all("eucan_persons", eucan_persons)
else:
    log.debug("\nNo new contacts to be added")
if len(eucan_events) > 0:
    log.debug("\nAdd %s Events", len(eucan_events))
    eucan_session.add_all("eucan_events", eucan_events)
else:
    log.debug("\nNo new events to be added")

if len(eucan_populations) > 0:
    log.debug("\nAdd %s Populations", len(eucan_populations))
    eucan_session.add_all("eucan_population", eucan_populations)
else:
    log.debug("\nNo new populations to be added")

log.debug("\nAdd %s Studies", len(eucan_studies))
eucan_session.add_all("eucan_study", eucan_studies)

print("FINISHED")
