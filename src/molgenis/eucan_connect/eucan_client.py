from typing import Dict, List, Optional
from urllib.parse import quote_plus

import requests

from molgenis.client import Session
from molgenis.eucan_connect import utils
from molgenis.eucan_connect.errors import EucanWarning
from molgenis.eucan_connect.model import (
    Catalogue,
    CatalogueData,
    IsoCountryData,
    RefData,
    RefEntity,
    RefTable,
    TableMeta,
)
from molgenis.eucan_connect.printer import Printer


class ExtendedSession(Session):
    """
    Class containing functionalities that the base molgenis python client Session class
    does not have. Methods in this class could be moved to molgenis-py-client someday.
    """

    def __init__(self, url: str, token: Optional[str] = None):
        super(ExtendedSession, self).__init__(url, token)
        self.url = url

    def add_batched(self, entity_type_id: str, entities: List[dict]):
        """Adds multiple entities in batches of 1000."""
        # TODO adding things in bulk will fail if there are self-references across
        #  batches. Dependency resolving is needed.
        batches = list(utils.batched(entities, 1000))
        for batch in batches:
            self.add_all(entity_type_id, batch)

    def get_meta(self, entity_type_id: str) -> TableMeta:
        """Similar to get_entity_meta_data() of the parent Session class, but uses the
        newer Metadata API instead of the REST API V1."""
        response = self._session.get(
            self._api_url + "metadata/" + quote_plus(entity_type_id),
            headers=self._get_token_header(),
        )
        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return TableMeta(meta=response.json())


class EucanSession(ExtendedSession):
    """
    A session with a EUCAN-Connect Catalogue. Contains methods to get source catalogues,
    their data and EUCAN-Connect data.
    """

    def __init__(self, *args, **kwargs):
        super(EucanSession, self).__init__(*args, **kwargs)
        self.printer = Printer()
        self.warnings: List[EucanWarning] = []

    CATALOGUES_TABLE = "eucan_source_catalogues"

    def get_catalogues(self, codes: List[str] = None) -> List[Catalogue]:
        """
        Retrieves a list of Source catalogues objects from the source catalogues table.
        Returns all source catalogues or some source catalogues if 'codes' is specified.
        :param codes: source catalogues to get by code
        :return: list of Catalogue objects
        """
        if codes:
            catalogues = self.get(self.CATALOGUES_TABLE, q=f"id=in=({','.join(codes)})")
        else:
            catalogues = self.get(self.CATALOGUES_TABLE)

        if codes:
            self._validate_codes(codes, catalogues)
        return self._to_catalogues(catalogues)

    def get_iso_country_data(self) -> IsoCountryData:
        """
        Retrieves all ISO two and three letter country codes and the country names
        stored in the EUCAN-Connect Catalogue
        :return: an IsoCountryData object
        """
        eucan_countries = self.get(
            "eucan_country",
            batch_size=10000,
            attributes="iso2_code,iso3_code,country_name,country_code",
        )

        iso_country_data = utils.to_upload_format(eucan_countries)

        return IsoCountryData(iso_country_data=iso_country_data)

    def get_reference_data(self) -> RefData:
        """
        Retrieves the current data from the reference entities
        in the EUCAN-Connect Catalogue
        :return: a RefData object
        """

        tables = dict()
        for ref_entity in RefEntity.get_ref_entities():
            id_ = ref_entity.base_id
            tables[ref_entity] = RefTable.of(
                table_type=ref_entity, rows=self._get_ref_entity_data(id_)
            )

        return RefData.from_dict(ref_entity=RefEntity, tables=tables)

    def set_study_mrefs(
        self, catalogue_data: CatalogueData, mref_data
    ) -> CatalogueData:
        """
        Fills the mrefs of the EUCAN-Connect Catalogue study data
        """
        for study in catalogue_data.studies.rows:
            no_prefix_id = study["id"][study["id"].find(":studyID:") + 9 :]
            for attribute in mref_data[no_prefix_id].keys():
                study[attribute] = mref_data[no_prefix_id][attribute]
        return catalogue_data

    def _get_ref_entity_data(self, entity_type_id: str) -> Dict:
        """
        Returns all the rows of an reference entity type
        :return: a list of dictionaries
        """

        eucan_ref_data = self.get(
            entity_type_id, batch_size=10000, attributes="id,label"
        )

        ref_data = utils.to_upload_format(eucan_ref_data)

        return ref_data

    @staticmethod
    def _to_catalogues(catalogues: List[dict]):
        """Maps rows to the Catalogue object."""
        result = list()
        for catalogue in catalogues:
            result.append(
                Catalogue(
                    code=catalogue["id"],
                    description=catalogue["description"],
                    catalogue_url=catalogue["catalogue_url"],
                    catalogue_type=catalogue["catalogue_type"],
                )
            )
        return result

    @staticmethod
    def _validate_codes(codes: List[str], catalogues: List[dict]):
        """Raises a KeyError if a requested source catalogue code was not found."""
        retrieved_codes = {catalogue["id"] for catalogue in catalogues}
        for code in codes:
            if code not in retrieved_codes:
                raise KeyError(f"Unknown code: {code}")
