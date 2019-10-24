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

from bs4 import BeautifulSoup
import requests
from typing import List

class OrgSynScrapper(object):
    ANNUAL_VOLUME_SELECT_ID = "ctl00_QuickSearchAnnVolList1"
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

        annualVolumes = []

        for option in annualVolSelect.findAll("option"):
            value = option["value"]
            if value:
                annualVolumes.append(value)

        return annualVolumes

if __name__ == "__main__":
    with OrgSynScrapper() as scrapper:
        pass
