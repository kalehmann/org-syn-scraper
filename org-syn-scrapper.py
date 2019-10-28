#! /usr/bin/env python3

# Copyright 2019 Karsten Lehmann <mail@kalehmann.de>

#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This script provides functionality to scrape all the PDF links from the site
http://orgsyn.org/ and download the PDF files.
"""

import argparse
from bs4 import BeautifulSoup, Tag
import json
import multiprocessing
import numpy
from pathlib import Path
import re
import requests
from typing import List, Tuple
import urllib.parse

class ScrapperParser(object):
    """Parser of command line arguments for the OrgSynScrapper."""
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="Scrape pdf links from http://orgsyn.org"
        )
        subparsers = self.parser.add_subparsers(
            dest="command",
            description="The following operation modes are available:"
        )
        subparsers.required = True

        dump_links = subparsers.add_parser(
            "dump_links",
            description="Dumps the PDF links from all or a single volume"
        )
        dump_links.add_argument(
            "--volume",
            dest="volume",
            help="The optional number of the annual volume to scrape for pdf links",
            required=False,
        )
        dump_links.add_argument(
            "--links-only",
            action="store_true",
            dest="links_only",
            help="Print only the links",
            required=False,
        )
        dump_links.add_argument(
            "--processes",
            default=4,
            dest="processes",
            help="The number of parallel processes",
            required=False,
            type=int
        )
        dump_links.set_defaults(func=self.dump_links)

    def dump_links(self, args):
        """Dumps the links of all pdf files in a volume or all volumes as json
        or plain text.

        :param args: The command line arguments for the dump_links function
        """
        annualVolumes = [args.volume]
        pdfDescriptions = []
        if args.volume is None:
            with OrgSynScrapper() as scrapper:
                annualVolumes = scrapper.requestVolumes()

        for volume in annualVolumes:
            pdfDescriptions += OrgSynScrapper.doLoadVolumePdfLinksParallel(
                volume,
                number_of_processes=args.processes
            )

        if args.links_only:
            for description in pdfDescriptions:
                print(description.url)
            return

        print(OrgSynScrapper.generateLinkJson(pdfDescriptions))

    def parse_args(self, *args, **kwargs):
        """Just a passtrough to the parse_args method of the ArgumentParser
        instance.

        See https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
        """
        return self.parser.parse_args(*args, **kwargs)

class PdfDescription(object):
    """Describes a PDF file on the server with the name of the file and a link
    to it.
    """
    def __init__(self, annualVolume : str, page : str, name : str, url : str):
        self.annualVolume = annualVolume
        self.name = name
        self.page = page
        self.url = url

    @property
    def slug(self):
        # See https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
        # See https://github.com/django/django/blob/master/django/utils/text.py
        s = str(self.name).strip().replace(' ', '_')

        return re.sub(r'(?u)[^-\w.]', '', s)

    def __repr__(self):
        data = {
          "annualVolume" : self.annualVolume,
          "name" : self.name,
          "page" : self.page,
          "slug" : self.slug,
          "url" : self.url,
        }

        return json.dumps(data, indent=2)

def volume_page_pdf_link_wrapper(
    data : Tuple[str, List[str]]
) -> List[PdfDescription]:
    """Wrapper for the doLoadVolumePagesPdfLinks class method of the
    OrgSynScrapper class. This function takes the doLoadVolumePagesPdfLinks
    methods arguments as a list and unpacks them. It is used as function
    for the map method of the multiprocessing.Pool class.

    :param data: a tuple with the doLoadVolumePagesPdfLinks arguments

    :return: A list with PdfDescription instances describing the files
    """
    return OrgSynScrapper.doLoadVolumePagesPdfLinks(*data)

class OrgSynScrapper(object):
    ANNUAL_VOLUME_SELECT_ID = "ctl00_QuickSearchAnnVolList1"
    PAGES_RESPONSE_OPTIONS_INDEX = 11
    PAGES_RESPONSE_VIEWSTATE_INDEX = 51
    PAGES_RESPONSE_VIEWSTATEGENERATOR_INDEX = 55
    PAGES_RESPONSE_EVENTVALIDATION_INDEX = 59
    URL = "http://orgsyn.org"
    USER_AGENT = "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"

    def __init__(self):
        self.session = None
        # The __VIEWSTATE value of OrgSyns formular
        self.viewstate = None
        # The __VIEWSTATEGENERATOR value of OrgSyns formular
        self.viewstategenerator = None
        # The __EVENTVALIDATION value of OrgSyns formular
        self.eventvalidation = None

    def __enter__(self) -> 'OrgSynScrapper':
        self.session = requests.session()
        self.session.headers.update({
	        "User-Agent" : OrgSynScrapper.USER_AGENT,
            "Accept" : "*/*",
            "Accept-Encoding" : "gzip,deflate,sdch",
            "Accept-Language" : "en-US,en;q=0.8",
        })

        return self

    def __exit__(self, type, value, traceback):
        self.session.close()

    @staticmethod
    def getInputValue(soup : BeautifulSoup, id : str) -> str:
        """Gets the value of the input element with the given id.

        :param soup: The BeautifulSoup instance to search for a input element
        with the given id
        :param id: The id of the input element

        :return: The value of the input element
        """
        input_el = soup.find('input', {"id" : id})
        if input_el:
            return input_el['value']
        return None

    @staticmethod
    def pdfLinkFilter(tag : Tag):
        """Filter for a BeautifulSoup instance for tags with a link to a pdf
        file in a folder named `Content`

        :param tag: The tag to check for a pdf link

        :returns: True if the tag links to a pdf file in a folder named Content
        else False
        """
        return (tag.has_attr("href") and tag["href"].startswith("Content")
            and tag["href"].endswith(".pdf"))

    def requestVolumes(self) -> List[str]:
        """Requests all annual volumes and sets the viewstate,
        viewstategenerator and eventvalidation attributes.

        :return: A list with all annual volumes as strings
        """
        response = self.session.get(OrgSynScrapper.URL)
        soup = BeautifulSoup(response.content, 'html.parser')

        self.viewstate = OrgSynScrapper.getInputValue(soup, "__VIEWSTATE")
        self.viewstategenerator = OrgSynScrapper.getInputValue(
            soup,
            "__VIEWSTATEGENERATOR"
        )
        self.eventvalidation = OrgSynScrapper.getInputValue(
            soup,
            "__EVENTVALIDATION"
        )

        annualVolSelect = soup.find(
            "select",
            {"id" : OrgSynScrapper.ANNUAL_VOLUME_SELECT_ID}
        )

        annualVolumes = map(
            lambda option: option["value"],
            annualVolSelect.findAll("option")
        )

        filtered_volumes = filter(
            lambda volume: volume,
            annualVolumes
        )

        return list(filtered_volumes)

    def requestPagesOfVolume(self, volume: str) -> List[str]:
        """Requests all pages of an annual volume.

        :param volume: The volume to request the pages for

        :return: A list with all the pages of the volume as strings
        """
        body = {
            "ctl00$ScriptManager1": "ctl00$UpdatePanel1|ctl00$QuickSearchAnnVolList1",
            "ctl00$QuickSearchAnnVolList1" : volume,
            "ctl00$tab2_TextBox": "",
            "ctl00$TBWE3_ClientState": "",
            "ctl00$SrcType": "Anywhere",
            "ctl00$MainContent$QSAnnVol": "Select Ann. Volume",
            "ctl00$MainContent$QSCollVol": "Select Coll. Volume",
            "ctl00$MainContent$searchplace": "publicationRadio",
            "ctl00$MainContent$TextQuickSearch": "",
            "ctl00$MainContent$TBWE2_ClientState": "",
            "ctl00$MainContent$SearchStructure": "",
            "ctl00$MainContent$SearchStructureMol": "",
            "ctl00$HidSrcType": "",
            "ctl00$WarningAccepted": "0",
            "ctl00$Direction": "",
            "__LASTFOCUS": "",
            "__EVENTTARGET": "ctl00$QuickSearchAnnVolList1",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstategenerator,
            "__EVENTVALIDATION": self.eventvalidation,
        }

        response = self.session.post(OrgSynScrapper.URL, data=body)
        content = str(response.content)
        options_html = str(response.content).split("|")[
            OrgSynScrapper.PAGES_RESPONSE_OPTIONS_INDEX
        ]
        optionsSoup = BeautifulSoup(options_html, "html.parser")
        pages = map(
            lambda option: option["value"],
            optionsSoup.findAll("option")
        )
        filtered_pages = filter(
            lambda page: page,
            pages
        )

        self.viewstate = str(response.content).split("|")[
            OrgSynScrapper.PAGES_RESPONSE_VIEWSTATE_INDEX
        ]
        self.viewstategenerator = str(response.content).split("|")[
            OrgSynScrapper.PAGES_RESPONSE_VIEWSTATEGENERATOR_INDEX
        ]
        self.eventvalidation = str(response.content).split("|")[
            OrgSynScrapper.PAGES_RESPONSE_EVENTVALIDATION_INDEX
        ]

        return list(filtered_pages)

    def requestVolumePagePdfLinks(
        self, volume : str, page : str
    ) -> List[PdfDescription]:
        """Request all pdf links for a page of a volume.

        :param volume: The volume
        :param page: The page of the volume to request the pdf links for

        :return: A list with PdfDescription instances describing the files
        """
        body = {
            "ctl00$QuickSearchAnnVolList1": volume,
            "ctl00$PageTextBoxDrop": page,
            "ctl00$tab2_TextBox": "",
            "ctl00$TBWE3_ClientState": "",
            "ctl00$SrcType": "Anywhere",
            "ctl00$MainContent$QSAnnVol": "Select Ann. Volume",
            "ctl00$MainContent$QSCollVol": "Select Coll. Volume",
            "ctl00$MainContent$searchplace": "publicationRadio",
            "ctl00$MainContent$TextQuickSearch": "",
            "ctl00$MainContent$TBWE2_ClientState": "",
            "ctl00$MainContent$SearchStructure": "",
            "ctl00$MainContent$SearchStructureMol": "",
            "ctl00$HidSrcType": "Citation",
            "ctl00$WarningAccepted": "1",
            "ctl00$Direction": "",
            "__LASTFOCUS": "",
            "__EVENTTARGET": "QuickSearchVolSrc",
            "__EVENTARGUMENT": "submitsearch",
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstategenerator,
            "__EVENTVALIDATION": self.eventvalidation,
        }

        response = self.session.post(
            OrgSynScrapper.URL,
            data=body,
            cookies={"quickSearchTab" : "0"}
        )

        url = response.url
        if url.endswith(".pdf"):
            # Check if the Server already redirected us to the PDF file.
            # This happens for example with volume 88 page 1.
            return [PdfDescription(volume, page, Path(url).stem, url)]

        soup = BeautifulSoup(response.content, "html.parser")
        link_tags = soup.find_all(OrgSynScrapper.pdfLinkFilter)
        links = list(map(
            lambda tag: urllib.parse.urljoin(OrgSynScrapper.URL, tag["href"]),
            link_tags
        ))

        title_tags = soup.select("#ctl00_MainContent_procedureBody > .title")

        if len(link_tags) == 0:
            # Maybe the page has a different layout like page 121 of volume 49
            # with two pdf files.
            id_tags = soup.find_all("div", {"class" : "collapsibleContainer" })
            links = list(map(
                lambda tag: urllib.parse.urljoin(
                    OrgSynScrapper.URL,
                    f"Content/pdfs/procedures/{tag['id']}.pdf"
                ),
                id_tags
            ))
            title_tags = soup.findAll("div", { "class" : "procTitle" })

        titles = list(map(
            lambda tag: tag.text.strip(),
            title_tags
        ))

        if len(titles) == len(links):
            return list(map(
                lambda title_url: PdfDescription(volume, page, *title_url),
                zip(titles, links)
            ))

        return list(map(
            lambda url: PdfDescription(volume, page, Path(url).stem, url),
            links
        ))

    @classmethod
    def doLoadVolumePagesPdfLinks(
        cls, volume : str, pages : List[str]
    ) -> List[PdfDescription]:
        """Get the pdf links for a given set of pages. Does the full procedure
        without the need for any preceeding method calls for the viewstate etc.

        :param volume: The volume of the pages
        :para pages: The pages of the volume to analyze for links

        :return: A list with PdfDescription instances describing the files
        """
        links = []

        with cls() as scrapper:
            volumes = scrapper.requestVolumes()
            if volume not in volumes:
                raise Exception(f"The volume {volume} does not exist")
            volume_pages = scrapper.requestPagesOfVolume(volume)
            for page in pages:
                if page not in volume_pages:
                    raise Exception(
                        f"The page {page} does not exist in volume {volume}"
                    )
                links += scrapper.requestVolumePagePdfLinks(volume, page)

        return links

    @classmethod
    def doLoadVolumePdfLinksParallel(
        cls, volume : str, number_of_processes : int = 4
    ) -> List[PdfDescription]:
        """Performs the requests for the pdf links of a volume parallel.

        :param volume: The volume to get the pdf links for
        :param number_of_processes: The number of parallel processes that
                                    request the links

        :return: A list with PdfDescription instances describing the files
        """
        links = []

        with cls() as scrapper:
            volumes = scrapper.requestVolumes()
            if volume not in volumes:
                raise Exception(f"The volume {volume} does not exist")
            pages = scrapper.requestPagesOfVolume(volume)

        page_chunks = numpy.array_split(pages, number_of_processes)

        with multiprocessing.Pool(processes=number_of_processes) as pool:
            result = pool.map(
                volume_page_pdf_link_wrapper,
                zip([volume] * number_of_processes, page_chunks)
            )

        for i in result:
            links += i

        return links

    def requestVolumePdfLinks(self, volume : str) -> List[PdfDescription]:
        """Requests the pdf links of all pages in a given volume.

        :param volume: the volume to get the pdf links for

        :return: A list with PdfDescription instances describing the files
        """
        pages = self.requestPagesOfVolume(volume)

        links = []

        for page in pages:
            links += self.requestVolumePagePdfLinks(volume, page)

        return links

    @staticmethod
    def generateLinkJson(links : List[PdfDescription]) -> str:
        """Generates a json string with the scheme
        ```
        [
            {
                "annualVolume" : "string",
                "page" : "string",
                "name": "string",
                "slug": "string",
                "url": "string"
            }
        ]
        ```

        :param links: A list of PdfDescription instances

        :returns: The json string
        """
        data = map(
            lambda description: {
                "annualVolume" : description.annualVolume,
                "page" : description.page,
                "name": description.name,
                "slug": description.slug,
                "url": description.url
            },
            links
        )

        return json.dumps(list(data), indent=2)

if __name__ == "__main__":
    parser = ScrapperParser()
    args = parser.parse_args()
    args.func(args)
