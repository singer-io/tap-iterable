#
# Module dependencies.
#

from datetime import datetime, timedelta
from singer import utils
from urllib.parse import urlencode
import backoff
import json
import requests
import logging


logger = logging.getLogger()



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
    total_days = (utils.strptime_with_tz(end_date) - utils.strptime_with_tz(start_date)).days
    remaining_days = int(total_days / self.api_window_in_days)
    if total_days >= self.api_window_in_days:
      for n in range(remaining_days):
        yield ((utils.strptime_with_tz(start_date) + n * timedelta(self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S"))
      if int(total_days % self.api_window_in_days) > 0 :
        start_slot_date = (utils.strptime_with_tz(start_date) + remaining_days * timedelta(self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S")
        yield (start_slot_date)
    else:
      yield utils.strptime_with_tz(start_date).strftime("%Y-%m-%d %H:%M:%S")


  def retry_handler(details):
    logger.info("Received 429 -- sleeping for %s seconds",
                details['wait'])

  @backoff.on_exception(backoff.expo,
                        requests.exceptions.HTTPError,
                        on_backoff=retry_handler,
                        max_tries=10)
  def _get(self, path, stream=True, **kwargs):
    """ The actual `get` request.  """
    uri = "{uri}{path}".format(uri=self.uri, path=path)

    # Add query params, including `api_key`.
    params = {}
    headers = {"api_key": self.api_key}
    for key, value in kwargs.items():
      params[key] = value
    uri += "?{params}".format(params=urlencode(params))
    logger.info("GET request to {uri}".format(uri=uri))

    response = requests.get(uri, stream=stream, headers=headers, params=params)
    logger.info("Response status:%s", response.status_code)

    response.raise_for_status()
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
      users = self._get("lists/getUsers", **kwargs)
      for user in users:
        yield {
          "email": user,
          "listId": l["id"],
          "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
        }


  def campaigns(self, column_name=None, bookmark=None):
    res = self.get("campaigns")
    for c in res["campaigns"]:
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
    for template_type in template_types:
      for medium in message_mediums:
        res = self.get("templates", templateTypes=template_type, messageMedium=medium)
        for t in res["templates"]:
          yield t


  def metadata(self, column_name=None, bookmark=None):
    tables = self.get("metadata")
    for t in tables["results"]:
      keys = self.get("metadata/{table_name}".format(table_name=t["name"]))
      for k in keys["results"]:
        value = self.get("metadata/{table_name}/{key}".format(table_name=k["table"], key=k["key"]))
        yield value


  def get_data_export_generator(self, data_type_name, bookmark=None):
    now = self._now()
    kwargs = {}
    for start_date_time in self._daterange(bookmark, now):
      kwargs["startDateTime"] = start_date_time
      endDateTime = (utils.strptime_with_tz(start_date_time) + timedelta(self.api_window_in_days)).strftime("%Y-%m-%d %H:%M:%S")
      if endDateTime <= now: 
        kwargs["endDateTime"] = endDateTime
      else :
        kwargs["endDateTime"] = now

      def get_data():
        return self._get("export/data.json", dataTypeName=data_type_name, **kwargs), kwargs['endDateTime']
      yield get_data
