#!/usr/bin/python
"""
InterAction:
------------

Reads InterAction HXLated csvs and creates datasets.

"""
import logging
from copy import deepcopy

from bs4 import BeautifulSoup
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from hdx.utilities.downloader import Download
from html2text import html2text
from slugify import slugify

logger = logging.getLogger(__name__)


class InterAction:
    def __init__(self, configuration):
        self.configuration = configuration
        self.headers = None
        self.countriesdata = dict()
        self.tagsdata = dict()
        self.tags = set()
        self.showcasedata = dict()
        self.countries_showcases = dict()
        self.datasets = dict()

    def get_tags(self, column):
        tags = set()
        if not column:
            return tags
        for text in column.split(","):
            text = text.replace("&amp;", "and")
            for tag in text.strip().split("and "):
                tag = tag.strip().lower()
                if tag:
                    tag = self.configuration["tag_mapping"].get(tag, tag)
                    tags.add(tag)
        return tags

    def add_tags(self, tags, countryiso):
        self.tags.update(tags)
        country_tagsdata = self.tagsdata.get(countryiso, set())
        country_tagsdata.update(tags)
        self.tagsdata[countryiso] = country_tagsdata

    def get_countriesdata(self, retriever):
        countrynameisomapping = dict()
        self.headers, iterator = retriever.get_tabular_rows(
            self.configuration["download_url"],
            format="csv",
            headers=1,
            dict_form=True,
        )

        with Download() as soup_downloader:
            soup_retriever = retriever.clone(soup_downloader)
            countries = list()
            for row in iterator:
                countrynames = row["Countries"]
                if not countrynames:
                    continue
                # Get urls for showcases and simplify titles of form:
                # <a href="/members/humanity-inclusion/population-shock" hreflang="en">A Population in Shock</a>
                soup = BeautifulSoup(row["Title"], features="lxml")
                anchor = soup.find("a")
                title = anchor.get_text()
                tags = {"hxl"}
                tags.update(self.get_tags(row["Sectors"]))
                tags.update(self.get_tags(row["Other Tags"]))
                tags = sorted(tags)
                showcase = self.showcasedata.get(title, dict())
                if not showcase:
                    showcase_url = f"https://www.ngoaidmap.org{anchor['href']}"
                    content = soup_retriever.download_text(
                        showcase_url, format="html"
                    )
                    soup = BeautifulSoup(content, features="lxml")
                    images = soup.findAll("img")
                    image_url = f"https://www.ngoaidmap.org{images[-1]['src']}"
                    name = slugify(title)
                    showcase = Showcase(
                        {
                            "name": name[:30],
                            "title": title,
                            "notes": "Click the image to go to the update",
                            "url": showcase_url,
                            "image_url": image_url,
                        }
                    )
                    showcase.add_tags(tags)
                    self.showcasedata[title] = showcase

                for origname in countrynames.split(","):
                    origname = origname.strip()
                    countryiso = countrynameisomapping.get(origname)
                    if countryiso is None:
                        countryiso, _ = Country.get_iso3_country_code_fuzzy(
                            origname, exception=ValueError
                        )
                        countryname = Country.get_country_name_from_iso3(
                            countryiso
                        )
                        countrynameisomapping[origname] = countryiso
                        countries.append(
                            (
                                countryiso,
                                countryname,
                                origname,
                            )
                        )
                    newrow = deepcopy(row)
                    newrow["iso3"] = countryiso
                    newrow["Title"] = title
                    del newrow["Countries"]

                    # Convert HTML formattted text to plain text
                    newrow["Summary"] = html2text(row["Summary"])

                    # Convert dates of form:
                    # <time datetime="2021-02-10T12:00:00Z" class="datetime">2021-02-10</time>
                    newrow["Date"] = row["Date"][-17:-7]

                    self.add_tags(tags, countryiso)

                    dict_of_lists_add(self.countriesdata, countryiso, newrow)
                    dict_of_sets_add(
                        self.countries_showcases, countryiso, title
                    )
            self.headers.append("iso3")
            countries = [
                {"iso3": x[0], "countryname": x[1], "origname": x[2]}
                for x in sorted(countries)
            ]
            return countries, self.showcasedata.values()

    def generate_dataset(self, folder, country):
        """ """
        countryiso = country["iso3"]
        countryname = country["countryname"]
        title = f"{countryname} - Work of InterAction Members"
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(f"Interaction Data for {countryname}").lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("b74dee39-1737-4784-83f6-644544b7a295")
        dataset.set_expected_update_frequency("Every month")
        dataset.set_subnational(False)
        dataset.add_country_location(countryiso)
        tags = ["hxl"] + list(self.tagsdata[countryiso])

        filename = f"member_data_{countryiso}.csv"
        resourcedata = {
            "name": f"InterAction Member Data for {countryname}",
            "description": "InterAction Member data with HXL tags",
        }

        success, results = dataset.generate_resource_from_iterator(
            self.headers,
            self.countriesdata[countryiso],
            self.configuration["hxltags"],
            folder,
            filename,
            resourcedata,
            "Date",
        )
        if success is False:
            logger.warning(f"{countryname} has no data!")
            return None, None
        dataset.add_tags(tags)

        self.datasets[countryiso] = dataset
        return dataset

    def get_showcases_for_dataset(self, dataset):
        countryiso = dataset.get_location_iso3s()[0].upper()
        titles = self.countries_showcases.get(countryiso, tuple())
        return [self.showcasedata[title] for title in titles]
