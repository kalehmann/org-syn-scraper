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
import datetime
import json
import multiprocessing
import numpy
from pathlib import Path
import os
import re
import requests
from requests.exceptions import RequestException
import sys
import time
from typing import Callable, List, Tuple
import urllib.parse
import urllib.request


class ProgressBar(object):
    """A quick and dirty progress bar for the terminal. Shows the progress in
    numbers and graphical.
    """
    PREFIX="[{current:>{width}}/{total}] "

    def __init__(self, total : int):
        """
        :param total: The total number of items that will be processed
        """
        self.progress = 0
        self.set_total(total)

    def set_total(self, total):
        """
        """
        self.total = total
        self.total_len = len(str(total))
        self.prefix_len = len(ProgressBar.PREFIX.format(
            current=0, width=self.total_len, total=self.total
        ))
        if total:
            self.print_progress()

    def print_progress(self):
        """(Re)prints the progress bar"""
        prefix = ProgressBar.PREFIX.format(
            current=self.progress,
            width=self.total_len,
            total=self.total
        )
        # The width of the bar is the total width minus the prefix length and
        # two characters for the square brackets enclosing the progress bar.
        width = os.get_terminal_size(0).columns - self.prefix_len - 2
        bar = "=" * int(width * self.progress / self.total - 1) + ">"
        sys.stdout.write(
            "\r{prefix}[{bar:<{width}}]".format(
                prefix=prefix,
                bar=bar,
                width=width,
            )
        )
        sys.stdout.flush()

    def increase(self):
        """
        Tells the progress bar, that one more item has been processed. This
        advances and redraws it.
        """
        self.progress += 1
        if self.progress > self.total:
            # Avoid weird behavior
            return
        self.print_progress()
        if self.progress == self.total:
            # Perform a line break if we are done.
            sys.stdout.write("\n")
            sys.stdout.flush()

class PdfDescription(object):
    """Describes a PDF file on the server with the name of the file and a link
    to it.
    """
    def __init__(self, annualVolume : str, page : str, name : str, url : str):
        self.aliases = []
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

    @property
    def downloadPath(self):
        return f"{self.annualVolume}/{self.page}/{self.slug}.pdf"

    def __repr__(self):
        data = {
          "aliases" : self.aliases,
          "annualVolume" : self.annualVolume,
          "name" : self.name,
          "page" : self.page,
          "slug" : self.slug,
          "url" : self.url,
        }

        return json.dumps(data, indent=2)

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

        download = subparsers.add_parser(
            "download",
            description="Dumps the PDF links from all or a single volume"
        )
        download.add_argument(
            "--volume",
            dest="volume",
            help="The optional number of the annual volume to scrape for pdf links",
            required=False,
        )
        download.add_argument(
            "--processes",
            default=4,
            dest="processes",
            help="The number of parallel processes",
            required=False,
            type=int
        )
        download.add_argument(
            "dest",
            help="The directory to download the pdf files into",
        )
        download.set_defaults(func=self.download)

    def fetch_links(
        self,
        volume : str = None,
        number_of_processes : int = 4,
        progress_bar : ProgressBar = None
    ) -> List[PdfDescription]:
        """Fetches the links for a given volume or all volumes if none is given
        parallely.

        :param volume: The volume to scrape for pdf links or None to scrape all
                       volumes
        :param number_of_processes: The number of parallel processes
        :param progress_bar: An optional ProgressBar instance for visual
                             progress tracking

        :return: A list with PdfDescription instances describing the files
        """
        annualVolumes = [volume]
        pdfDescriptions = []
        if volume is None:
            with OrgSynScrapper() as scrapper:
                annualVolumes = scrapper.requestVolumes()

        if progress_bar:
            progress_bar.set_total(len(annualVolumes))

        for volume in annualVolumes:
            pdfDescriptions += OrgSynScrapper.doLoadVolumePdfLinksParallel(
                volume,
                number_of_processes=number_of_processes
            )
            if progress_bar:
                progress_bar.increase()

        return OrgSynScrapper.deduplicateLinks(pdfDescriptions)

    def dump_links(self, args):
        """Dumps the links of all pdf files in a volume or all volumes as json
        or plain text.

        :param args: The command line arguments for the dump_links function
        """
        pdfDescriptions = self.fetch_links(args.volume, args.processes)

        if args.links_only:
            for description in pdfDescriptions:
                print(description.url)
            return

        print(OrgSynScrapper.generateLinkJson(pdfDescriptions))

    def download(self, args):
        volume_progress_bar = ProgressBar(0)
        file_progress_bar = ProgressBar(0)

        print("Scraping volumes for links:")
        pdfDescriptions = self.fetch_links(
            args.volume,
            number_of_processes=args.processes,
            progress_bar=volume_progress_bar
        )
        print(f"Found {len(pdfDescriptions)} links.")

        print("Downloading files")
        OrgSynScrapper.downloadPdfFilesParallel(
            pdfDescriptions,
            args.dest,
            number_of_processes=args.processes,
            progress_bar=file_progress_bar
        )


    def parse_args(self, *args, **kwargs):
        """Just a passtrough to the parse_args method of the ArgumentParser
        instance.

        See https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
        """
        return self.parser.parse_args(*args, **kwargs)

class OrgSynScrapper(object):
    ANNUAL_VOLUME_SELECT_ID = "ctl00_QuickSearchAnnVolList1"
    PAGES_RESPONSE_OPTIONS_INDEX = 11
    PAGES_RESPONSE_VIEWSTATE_INDEX = 51
    PAGES_RESPONSE_VIEWSTATEGENERATOR_INDEX = 55
    PAGES_RESPONSE_EVENTVALIDATION_INDEX = 59
    REQUEST_TIMEOUT = 15
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
        for i in range(1, 5):
            try:
                response = self.session.get(
                    OrgSynScrapper.URL,
                    timeout=OrgSynScrapper.REQUEST_TIMEOUT
                )
                break
            except RequestException as e:
                print(
                    f"[{datetime.datetime.now().ctime()}] An exception occured while requesting all the volumes {str(e)}. Try again in {i * 10} seconds",
                    file=sys.stderr
                )
                time.sleep(i * 10)
        else:
            print(
                f"[{datetime.datetime.now().ctime()}] Error: Could not fetch the volumes after 5 tries.",
                file=sys.stderr
            )

            return []

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

        for i in range(5):
            try:
                response = self.session.post(
                    OrgSynScrapper.URL,
                    data=body,
                    timeout=OrgSynScrapper.REQUEST_TIMEOUT
                )
                break
            except RequestException as e:
                print(
                    f"[{datetime.datetime.now().ctime()}] An exception occured while fetching the pages of volume {volume} : {str(e)}. Try again in {i * 10} seconds",
                    file=sys.stderr
                )
                time.sleep(i * 10)
        else:
            print(
                f"[{datetime.datetime.now().ctime()}] Error: Could not fetch the pages of volume {volume} after 5 tries.",
                file=sys.stderr
            )

            return []
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

        for i in range(5):
            try:
                response = self.session.post(
                    OrgSynScrapper.URL,
                    cookies={"quickSearchTab" : "0"},
                    data=body,
                    timeout=OrgSynScrapper.REQUEST_TIMEOUT,
                )
                break
            except RequestException as e:
                print(
                    f"[{datetime.datetime.now().ctime()}] An exception occured while fetching page {page} of volume {volume} : {str(e)}. Try again in {i * 10} seconds",
                    file=sys.stderr
                )
                time.sleep(i * 10)
        else:
            print(
                f"[{datetime.datetime.now().ctime()}] Error: Could not fetch the page {page} of volume {volume} after 5 tries.",
                file=sys.stderr
            )

            return []

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

    @staticmethod
    def downloadPdfFile(args: Tuple[str, PdfDescription]) -> bool:
        """
        """
        dest_dir, description = args

        path = os.path.join(dest_dir, description.downloadPath)

        try:
            urllib.request.urlretrieve(description.url, path)

            return True
        except:
            return False

    @classmethod
    def downloadPdfFilesParallel(
        cls,
        links : List[PdfDescription],
        dest_dir : str,
        number_of_processes : int = 4,
        progress_bar : ProgressBar = None
    ) -> None:
        """Download a list of pdf links to the local hard drive.

        :param links: A list with PdfDescription instances describing the files
                      to download
        :param dest_dir: The of the directory in which the files should be
                         downloaded
        :param progress_bar: An optional ProgressBar instance for visual
                             progress tracking
        """
        dirs = set()

        for link in links:
            dir = os.path.join(dest_dir, f"{link.annualVolume}/{link.page}")
            dirs.add(dir)

        for dir in dirs:
            os.makedirs(dir, exist_ok=True)

        if progress_bar:
            progress_bar.set_total(len(links))

        with multiprocessing.Pool(processes=number_of_processes) as pool:
            result = pool.imap_unordered(
                cls.downloadPdfFile,
                zip([dest_dir] * len(links), links)
            )

            for res in result:
                if progress_bar:
                    progress_bar.increase()

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
            result = pool.starmap(
                OrgSynScrapper.doLoadVolumePagesPdfLinks,
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
    def deduplicateLinks(links : List[PdfDescription]) -> List[PdfDescription]:
        """Removes duplicate links from a list of pdf descriptions.

        :params links: The list of pdf descriptions to deduplicate

        :return: A deduplicated list of pdf descriptions
        """
        deduplicated_descriptions = []

        for description in links:
            for dedup_descs in deduplicated_descriptions:
                if dedup_descs.url == description.url:
                    if dedup_descs.name == description.name:
                        break
                    if description.name in dedup_descs.aliases:
                        break
                    dedup_descs.aliases.append(description.name)
                    break
            else:
                deduplicated_descriptions.append(description)

        return deduplicated_descriptions

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
                "aliases" : description.aliases,
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
