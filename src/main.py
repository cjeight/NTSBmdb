
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

2020/03/26 - Marc Davidson:  Fixed the issue with the directory errors
            tweaked and documented the code.
"""
import sys
import zipfile
import os

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import re

# global variables
global file_path


def get_last_upd_date() -> datetime:
    """
    :description: Read the date of the last file downloaded from the NTSB website
                  and convert it to type datetime.
    :rtype: datetime
    :return: the date of the last update downloaded.
    """
    try:
        with open("updates.txt", "r") as f:
            record = f.readline()
    except:
        print("Oops!", sys.exc_info()[0], "occured while reading the last update date.")
        sys.exit(2)

    upd_date = record.split(",")[0]
    return datetime.strptime(upd_date, "%m/%d/%Y")


def compare_lst(d_list: list, lst_upd: datetime) -> list:
    """
    :description: compare the last update date to the list of available updates
                  from the NTSB website.
    :param d_list: list of available updates from the NTSB web site.
    :type d_list: list
    :param lst_upd: The date of the last sucessful database update.
    :type lst_upd: datetime
    :return: a list of updates that need to be applied.
    :rtype: list
    """

    cur_update = []

    for cur_upd in d_list:
        cur_date = datetime.strptime(cur_upd[0], "%m/%d/%Y")
        if lst_upd < cur_date:
            cur_update.append(cur_upd)

    return cur_update


def downloadupdate(udfname: str) -> bool:
    """
    :description: Download the latest update file from the NTSB webpage.

    :param udfname: Latest update filename
    :rtype udfname: text
    :return: path and udfname
    """

    url = r"https://app.ntsb.gov/avdata/Access/" + udfname
    udfname = file_path + "\\" + udfname
    r = requests.get(url, stream=True)
    if r.status_code == requests.codes.ok:
        try:
            with open(udfname, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        except:
            print("Oops!", sys.exc_info()[0], "occured while writing the update ZIP.")
            sys.exit(3)
        return True
    else:
        return False


def unzip(source_filename: str, dest_dir: str):
    """
    :discription: The downloaded file is a ZIP archive.  Unzip the zip file.
    :param source_filename: file to be unziped
    :param dest_dir: the path to unzip the file to
    :return: none
    """
    try:
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
    except:
        print("Oops!", sys.exc_info()[0], "occured while extracting the update file.")
        sys.exit(7)


def parsedata(lline: str) -> list:
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
            # dllist.append((recdate, tmp[4]))
            dllist.append((record[:10].strip(), tmp[4]))
    return dllist


def save_the_date(save_lst_upd: tuple):
    """
    :description: Save the date and file name of the last update.
    :param save_lst_upd: the date and filename of the last update.
    :return:
    """
    save_date, save_fname = save_lst_upd

    try:
        with open("updates.txt", "w") as f:
            f.write(f"{save_date}, {save_fname}")
    except:
        print("Oops!", sys.exc_info()[0], "occurred while saving the update date.")
        sys.exit(5)


def remove_file(afile: str):
    """
    :discription: Delete a file.
    :argument afile: the name of a file to remove.
    :returns nothing
    """
    try:
        if os.path.isfile(afile):
            os.remove(afile)
    except:
        print("Oops!", sys.exc_info()[0], "occured.")
        sys.exit(3)


def web_page_data() -> str:
    """
    :description: Scrape the NTSB update webpage to get updates available
                  for downloading.
    :return: Raw data from the website or an empty string if scrape fails.
    """
    url = 'https://app.ntsb.gov/avdata/Access/'     # NTSB source for update files
    r = requests.get(url)                           # Get a copy of the webpage HTML
    if r.status_code == requests.codes.ok:          # anything except 200 is a failure
        data = r.text                               # filter page
        soup = BeautifulSoup(data, "html.parser")   # more filtering
        data_line = soup.get_text()                 # even more filtering
        return data_line
    else:
        return ""                                   # scrape failed


def make_update_file(zip_file: str):
    """
        unzip the zip file downloaded from the NTSB web page, rename
        the file to match the DSN in the ODBC Connection Mgr. Remove
        the zip file before exiting.
        :argument zip_file: name of the downloaded zip file.
        :returns nothing
    """
    unzip(zip_file, file_path)
    #
    odbc_file = os.path.join(file_path, "NTSBupd.mdb")
    mdb_file = os.path.splitext(zip_file)[0] + ".mdb"

    remove_file(odbc_file)                              # make sure there's not an old copy of NTSBupd in the directory.

    try:
        os.rename(mdb_file, odbc_file)                  # rename file so ODBC connection mgr will see the data file.
    except:
        print("Oops!", sys.exc_info()[0], "occured while renaming the update file.")
        sys.exit(4)

    remove_file(zip_file)                               # remove the unzipped update file.

    print(f"{os.path.split(zip_file)[1]} downloaded to {file_path}, "
          f"unzipped and renamed to {os.path.split(odbc_file)[1]}.")


if __name__ == '__main__':
    """
    Access the NTSB database update webpage and capture the list
    of currently available updates.  Download the latest update
    and apply the update to the ToxFLO_NTSB database in the ToxDB
    system.

    :return: Nothing.
    """
    file_path = os.getcwd()                                 # capture the current path/directory
    lst_update = get_last_upd_date()                        #
    line = web_page_data()                                  # download the html from the NTSB updates page
    if len(line) > 0:
        # parse out the data to process
        dlist = parsedata(line)                             # makes the list of available updates
        # compare the list of available updates from the NTSB website against the last update date.
        new_updates = compare_lst(dlist, lst_update)
        if len(new_updates) == 0:
            print("You are up to-date.")
        else:
            for update in new_updates:
                if downloadupdate(update[1]):               # download the currently available NTSB update file.
                    save_the_date(update)
                    make_update_file(update[1])             # unzip, rename and to prepare for the ODBC mgr.
                else:
                    print(f"Download of {update} failed.")
                    sys.exit(9)
    else:
        print(f"Scrape of the NTSB webpage FAILED.")
        sys.exit(8)
