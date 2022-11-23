from datetime import date
from datetime import timedelta


def year(d):
    return d.year


def quarter(d):
    month = d.month
    if month < 4:
        return 1
    elif month < 7:
        return 2
    elif month < 10:
        return 3
    else:
        return 4
