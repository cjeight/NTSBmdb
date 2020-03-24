
"""
Author: Marc Davidson
Date: 12/04/2019

This should have been a simple issue of just forming the current month
and week to make a file name.  However, occasionaly the NTSB puts an
update out that is out of synce with the normal month/week format.
As a result this program to scrape the NTSB update web page and ferret
out the current update file name by the posted date rather than by name.

2020/02/04 - Marc Davidson:  To access the Access MDB file an ODBC
            connection is required.  For that reason code has been
            added to make sure there is not an existing renamed
            update file (NTSBupd.mdb) in the directory and code
            to rename the downloaded file to NTSBupd.mdb.

2020/03/23 - Marc Davidson:  Added the abillity to maintain a list
            of updates that have been downloaded so updates will
            not get skipped
"""

import zipfile
import os

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import re

# global variables
global g_tank_path


def downloadupdate(udfname: str):
    """
    Download the latest update file from the NTSB webpage.

    :param udfname: Latest update filename
    :rtype udfname: text
    :return: path and udfname
    """

    url = r"https://app.ntsb.gov/avdata/Access/" + udfname
    # TODO: look up the current directory and add \tank to the path.
    udfname = g_tank_path + "\\" + udfname
    # udfname = r"w:\repo\scrapeNTSB\tank" + "\\" + udfname
    print("")
    r = requests.get(url, stream=True)
    with open(udfname, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return udfname


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        for member in zf.infolist():
            # Path traversal defense
            words = member.filename.split('/')
            path = dest_dir
            for word in words[:-1]:
                drive, word = os.path.splitdrive(word)
                head, word = os.path.split(word)
                if word in (os.curdir, os.pardir, ''):
                    continue
                path = os.path.join(path, word)
            zf.extract(member, path)


def parsedata(lline):
    """
    Parse out and convert the text date to type date and capture the filename

    :param  lline: line of text returned from the NTSB webpage
            containing update dates, time, size and filename.
    :return: dllist : <list>
    """

    dllist = []
    lline = lline[124:]
    current_year = datetime.now().year
    last_year = current_year - 1

    # walk down the line picking off update filename and date.
    for match in re.finditer('zip', lline):
        record = lline[match.end() - 44:match.end()].strip()

        recdate = datetime.strptime(record[0:10].strip(), '%m/%d/%Y')
        if recdate.year in (last_year, current_year):
            tmp = record.split()
            dllist.append((recdate, tmp[4]))

    return dllist


def save_file(d_list):
    with open("updates.txt","w") as f:
        for line in d_list:
            file_date = str(line[0])
            file_name = line[1]
            f.write(f"{file_date},{file_name}\r")


def getcurrentupdate(d_list):
    """
    Sort the list of lists in reverse order and return the filename if the first position
    :param d_list: list of dates and filenames from NTSB webpage
    :return: update filename
    """
    d_list.sort(reverse=True)
    save_file(d_list)
    update, upfilename = d_list[0]
    print(f"The update file0name to download: {upfilename}.")

    return upfilename


def remove_file(afile):
    """
    :argument afile: then name of a file to remove.
    :returns nothing
    """
    if os.path.isfile(afile):
        os.remove(afile)


def web_page_data():
    url = 'https://app.ntsb.gov/avdata/Access/'  # NTSB source for update files
    r = requests.get(url)  # Get a copy of the webpage HTML
    data = r.text  # filter page
    soup = BeautifulSoup(data, "html.parser")  # more filtering
    data_line = soup.get_text()  # even more filtering
    return data_line


def make_update_file(zip_file):
    """
        unzip the zip file downloaded from the NTSB web page, rename
        the file to match the DSN in the ODBC Connection Mgr. Remove
        the zip file before exiting.
        :argument zip_file: name of the downloaded zip file.
        :returns nothing
    """
    unzip(zip_file, g_tank_path)
    #
    odbc_file = os.path.join(g_tank_path, "NTSBupd.mdb")
    mdb_file = os.path.splitext(zip_file)[0] + ".mdb"
    # make sure there's not an old copy of NTSBupd in the directory.
    remove_file(odbc_file)
    # rename file so ODBC connection mgr will see the data file.
    os.rename(mdb_file, odbc_file)
    remove_file(zip_file)                                           # remove the unzipped update file.

    print(f"{os.path.split(zip_file)[1]} downloaded to {g_tank_path}, unzipped and renamed to {os.path.split(odbc_file)[1]}.")


if __name__ == '__main__':
    """
    Access the NTSB database update webpage and capture the list
    of currently available updates.  Download the latest update
    and apply the update to the ToxFLO_NTSB database in the ToxDB
    system.

    :return: Nothing.
    """
    g_tank_path = "."    # global variable for path to working storage.
    # download the html from the NTSB updates page
    line = web_page_data()
    # parse out the data to process
    dlist = parsedata(line)
    # Ok now get the name of the most current NTSB update file available.
    updatefile = getcurrentupdate(dlist)
    # download the currently available NTSB update file.
    updatefile = downloadupdate(updatefile)
    # unzip, rename and to prepare for the ODBC mgr.
    make_update_file(updatefile)
