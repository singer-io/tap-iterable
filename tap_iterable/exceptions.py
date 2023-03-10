class IterableError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class IterableBadRequestError(IterableError):
    pass

class IterableServer5xxError(IterableError):
    pass

class IterableRateLimitError(IterableError):
    pass


class IterableUnauthorizedError(IterableError):
    pass


class IterableTooManyError(IterableRateLimitError):
    pass


class IterableNotAvailableError(IterableServer5xxError):
    pass



ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": IterableBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": IterableUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    429: {
        "raise_exception": IterableTooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    503: {
        "raise_exception": IterableNotAvailableError,
        "message": "API service is currently unavailable."
    }
}