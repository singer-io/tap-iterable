import requests


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
        "raise_exception": IterableRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    503: {
        "raise_exception": IterableNotAvailableError,
        "message": "API service is currently unavailable."
    }
}

def raise_for_error(response):   
    try:
        response.raise_for_status()
    except requests.HTTPError:
        try:
            json_resp = response.json()
        except (ValueError, TypeError, IndexError, KeyError):
            json_resp = {}

        error_code = response.status_code
        message_text = json_resp.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
        message = "HTTP-error-code: {}, Error: {}".format(error_code, message_text)
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", IterableError)
        raise exc(message) from None