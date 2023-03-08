#!/usr/bin/python
"""
Unit tests for InterAction.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from interaction import InterAction


class TestInterAction:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        tags = ["economics", "environment", "hxl"]
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_generate_datasets_and_showcases(
        self, configuration, fixtures, input_folder
    ):
        with temp_dir(
            "test_interaction", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                interaction = InterAction(configuration)
                countries, showcases = interaction.get_countriesdata(retriever)
                assert countries[0] == {
                    "iso3": "AFG",
                    "countryname": "Afghanistan",
                    "origname": "Afghanistan",
                }
                assert countries[15] == {
                    "iso3": "IDN",
                    "countryname": "Indonesia",
                    "origname": "Indonesia",
                }
                assert countries[32] == {
                    "iso3": "PSE",
                    "countryname": "State of Palestine",
                    "origname": "Palestinian Territory",
                }
                assert countries[49] == {
                    "iso3": "ZAF",
                    "countryname": "South Africa",
                    "origname": "South Africa",
                }
                locations = [
                    {"name": x["iso3"].lower(), "title": x["countryname"]}
                    for x in countries
                ]
                Locations.set_validlocations(locations)
                assert interaction.headers == [
                    "Title",
                    "Member Organization",
                    "Date",
                    "Countries",
                    "Summary",
                    "Content",
                    "Topics",
                    "Working Groups",
                    "Sectors",
                    "Funding Type",
                    "Other Tags",
                    "Author",
                    "Status",
                    "iso3",
                ]
                showcases = list(showcases)
                assert showcases[0] == {
                    "name": "building-resilience-of-the-blu",
                    "title": "Building Resilience of the Blue Economy in Latin America and Caribbean Region\xa0",
                    "notes": "Click the image to go to the update",
                    "url": "https://www.ngoaidmap.org/members/goal-global/building-resilience-blue-economy-latin-america-and-caribbean-region",
                    "image_url": "https://www.ngoaidmap.org/sites/default/files/styles/large/public/2021-10/IA_Sandipani%20Chattopadhy%20Blue%20Economy.jpeg?itok=hLvdu9hW",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                }
                assert len(showcases) == 136
                dataset = interaction.generate_dataset(folder, countries[0])
                assert dataset == {
                    "name": "interaction-data-for-afghanistan",
                    "title": "Afghanistan - Work of InterAction Members",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "owner_org": "b74dee39-1737-4784-83f6-644544b7a295",
                    "data_update_frequency": "30",
                    "subnational": "0",
                    "groups": [{"name": "afg"}],
                    "dataset_date": "[2021-02-10T00:00:00 TO 2021-02-10T23:59:59]",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                }
                resources = dataset.get_resources()
                assert len(resources) == 1
                resource = resources[0]
                assert resource == {
                    "name": "InterAction Member Data for Afghanistan",
                    "description": "InterAction Member data with HXL tags",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                filename = "member_data_AFG.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)
