# pylint: disable=E1136, E0213
#
# Module dependencies.
#

from datetime import datetime, timedelta
from singer.utils import strptime_with_tz, strftime
from urllib.parse import urlencode
import backoff
import json
import requests
import logging
import tap_iterable.helper as helper
from tap_iterable.exceptions import IterableRateLimitError, IterableNotAvailableError, raise_for_error

LOGGER = logging.getLogger()


class Iterable(object):
  """ Simple wrapper for Iterable. """

  def __init__(self, api_key, start_date=None, api_window_in_days=30):
    self.api_key = api_key
    self.uri = "https://api.iterable.com/api/"
    self.api_window_in_days = float(api_window_in_days)
    self.MAX_BYTES = 10240
    self.CHUNK_SIZE = 512


  def _now(self):
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


  def _daterange(self, start_date, end_date):
    total_days = (strptime_with_tz(end_date) - strptime_with_tz(start_date)).days
    remaining_days = int(total_days / self.api_window_in_days)
    if total_days >= self.api_window_in_days:
      for n in range(remaining_days):
        yield ((strptime_with_tz(start_date) + n * timedelta(self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S"))
      if int(total_days % self.api_window_in_days) > 0 :
        start_slot_date = (strptime_with_tz(start_date) + remaining_days * timedelta(self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S")
        yield (start_slot_date)
    else:
      yield strptime_with_tz(start_date).strftime("%Y-%m-%d %H:%M:%S")

  @backoff.on_exception(backoff.constant,
                        (IterableRateLimitError, IterableNotAvailableError),
                        jitter=None,
                        interval=30,
                        max_tries=5)
  def _get(self, path, stream=True, **kwargs):
    """ The actual `get` request.  """
    uri = "{uri}{path}".format(uri=self.uri, path=path)

    # Add query params, including `api_key`.
    params = {}
    headers = {"api_key": self.api_key}
    for key, value in kwargs.items():
      params[key] = value
    uri += "?{params}".format(params=urlencode(params))
    LOGGER.info("GET request to {uri}".format(uri=uri))

    response = requests.get(uri, stream=stream, headers=headers, params=params)
    LOGGER.info("Response status:%s", response.status_code)

    raise_for_error(response)

    return response


  def get(self, path, **kwargs):
    """" The common `get` request. """
    response = self._get(path, **kwargs)
    return response.json()


  def get_user_fields(self):
    """Get custom user fields, used for generating `users` schema in `discover`."""
    return self.get("users/getFields")


  #
  # Methods to retrieve data per stream/resource.
  #

  def lists(self, column_name=None, bookmark=None):
    res = self.get("lists")
    for l in res["lists"]:
      yield l


  def list_users(self, column_name=None, bookmark=None):
    res = self.get("lists")
    for l in res["lists"]:
      kwargs = {
        "listId": l["id"]
      }
      users = [x for x in self._get(
          "lists/getUsers", **kwargs).content.decode().split('\n') if x.strip()]
      for user in users:
        yield {
          "email": user,
          "listId": l["id"],
          "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
        }


  def campaigns(self, column_name=None, bookmark=None):
    bookmark = strptime_with_tz(bookmark)
    res = self.get("campaigns")
    for c in res["campaigns"]:
      rec_date_time = strptime_with_tz(helper.epoch_to_datetime_string(c["updatedAt"]))
      if rec_date_time >= bookmark:
        yield c


  def channels(self, column_name, bookmark):
    res = self.get("channels")
    for c in res["channels"]:
      yield c


  def message_types(self, column_name=None, bookmark=None):
    res = self.get("messageTypes")
    for m in res["messageTypes"]:
      yield m


  def templates(self, column_name, bookmark):
    template_types = [
      "Base",
      "Blast",
      "Triggered",
      "Workflow"
    ]
    message_mediums = [
      "Email",
      "Push",
      "InApp",
      "SMS"
    ]
    # `templates` API bug where it doesn't extract the records 
    #  where `startDateTime`= 2023-03-01+07%3A31%3A15 though record exists
    #  hence, substracting one second so that we could extract atleast one record 
    bookmark_val = strptime_with_tz(bookmark) - timedelta(seconds=1)
    bookmark = strftime(bookmark_val)
    for template_type in template_types:
      for medium in message_mediums:
        for kwargs in self.get_start_end_date(bookmark):
          res = self.get("templates", templateTypes=template_type, messageMedium=medium, **kwargs)
          for t in res["templates"]:
            rec_date_time = strptime_with_tz(helper.epoch_to_datetime_string(t["updatedAt"]))
            if rec_date_time >= bookmark_val:
              yield t


  def metadata(self, column_name=None, bookmark=None):
    tables = self.get("metadata")
    for t in tables["results"]:
      keys = self.get("metadata/{table_name}".format(table_name=t["name"]))
      for k in keys["results"]:
        value = self.get("metadata/{table_name}/{key}".format(table_name=k["table"], key=k["key"]))
        yield value


  def get_start_end_date(self, bookmark):
    now = self._now()
    kwargs = {}
    for start_date_time in self._daterange(bookmark, now):
      kwargs["startDateTime"] = start_date_time
      endDateTime = (strptime_with_tz(start_date_time) + timedelta(
        self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S")
      if endDateTime <= now:
        kwargs["endDateTime"] = endDateTime
      else:
        kwargs["endDateTime"] = now
      yield kwargs


  def get_data_export_generator(self, data_type_name, bookmark=None):
    for kwargs in self.get_start_end_date(bookmark):
      def get_data():
        return self._get("export/data.json", dataTypeName=data_type_name, **kwargs), kwargs['endDateTime']
      yield get_data      