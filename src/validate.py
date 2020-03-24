#!/usr/bin/python
'''
    Read the date of the last update from updates.txt file.
    Use the file date & name list scrapped off of the NTSB website.
        Sorted in reverse order.
    Traverse the list comparing the last update date to the scrapped list date.
        Make a new list of dates greater than the last update.





'''
from datetime import datetime
# from main import web_page_data, parsedata


# TODO: 2020-03-22 00:00:00,up22Mar.zip
def get_last_upd_date():
    with open("updates.txt", "r") as f:
        record = f.readline()

    upd_date = record[0:10]
    # file_name = record[20:-1]
    date_upd = datetime.strptime(upd_date, "%Y/%m/%d")
    print(f"{date_upd}")
    return date_upd


def compare_lst(d_list, lst_upd):
    '''
    :description: compare the last update date to the list of available updates
                  from the NTSB website.
    :param d_list: list of available updates from the NTSB web site.
    :type d_list: list
    :param lst_upd: The date of the last sucessful database update.
    :type lst_upd: datetime
    :return: a list of updates that need to be applied.
    :rtype: list
    '''

    # update = []
    # # i = 0
    # for cur_upd in d_list:
    #     if lst_upd < cur_upd:
    #         update.append(cur_upd)
    #         # update = (cur_upd[i][0], cur_upd[i][1])
    #
    #         # i += 1
    #     else:
    #         return update
    print(f"{d_list}")
    for cur in d_list:
        print(f"{lst_upd} -- {cur}\n")

    print("dummy")



# if __name__ == '__main__':
#     # temp = web_page_data()
#     # dlist = parsedata(temp)
#     last_upd = get_last_upd_date()
#     compare_lst(dlist, last_upd)
