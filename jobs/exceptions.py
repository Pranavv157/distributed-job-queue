class RetryableError(Exception):
    pass


class PermanentError(Exception):
    pass


class LeaseLostException(Exception):
    pass