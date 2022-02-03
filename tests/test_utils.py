import pytest

from molgenis.eucan_connect import utils


@pytest.fixture
def batch():
    total_list = []
    for i in range(0, 3000):
        total_list.append("A" + str(i))
    return total_list


def test_batched(batch):
    batches = list(utils.batched(batch, 1000))
    assert len(batches) == 3
    assert len(batches[0]) == 1000
    assert len(batches[1]) == 1000
    assert len(batches[2]) == 1000


@pytest.fixture
def rows():
    return [
        {
            "_href": "/api/v2/test_study/studyA",
            "id": "studyA",
            "population": {
                "_href": "/api/v2/test_population/populationA",
                "id": "populationA",
            },
            "data_collection_events": [],
        },
        {
            "_href": "/api/v2/test_study/studyB",
            "id": "studyB",
            "data_collection_events": [
                {"_href": "/api/v2/test_events/dce_A", "id": "dce_A"}
            ],
        },
    ]


def test_to_upload_format(rows):
    assert utils.to_upload_format(rows) == [
        {
            "id": "studyA",
            "population": "populationA",
            "data_collection_events": [],
        },
        {"id": "studyB", "data_collection_events": ["dce_A"]},
    ]
