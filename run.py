#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir, temp_dir
from hdx.utilities.retriever import Retrieve

from interaction import InterAction

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-interaction"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-sv",
        "--save",
        default=False,
        action="store_true",
        help="Save downloaded data",
    )
    parser.add_argument(
        "-usv",
        "--use_saved",
        default=False,
        action="store_true",
        help="Use saved data",
    )
    args = parser.parse_args()
    return args


def main(save, use_saved, **ignore):
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    with temp_dir(lookup) as folder:
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            interaction = InterAction(configuration)
            countries, showcases = interaction.get_countriesdata(retriever)
            for showcase in showcases:
                showcase.create_in_hdx()
            logger.info(f"Number of countries: {len(countries)}")
            for info, country in progress_storing_tempdir(
                "InterAction", countries, "iso3"
            ):
                folder = info["folder"]
                dataset = interaction.generate_dataset(folder, country)
                if dataset:
                    dataset.update_from_yaml()
                    dataset["notes"] = dataset["notes"].replace(
                        "\n", "  \n"
                    )  # ensure markdown has line breaks
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: InterAction",
                        batch=info["batch"],
                    )
                    for showcase in interaction.get_showcases_for_dataset(
                        dataset
                    ):
                        showcase.add_dataset(dataset)


if __name__ == "__main__":
    args = parse_args()
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        save=args.save,
        use_saved=args.use_saved,
    )
