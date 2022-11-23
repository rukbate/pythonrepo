from repository.data import dailyk


class Stock:

    code = None

    def __init__(self, code):
        self.code = code

    def daily_k(self, start_date=None, end_date=None):
        return dailyk.retrieve_daily_k(self.code, start_date, end_date)
