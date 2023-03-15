# tap-iterable

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [iterable](https://api.iterable.com/api/docs)
- Extracts the following resources:
  + campaigns
  + channels
  + email_bounce
  + email_click
  + email_complaint
  + email_open
  + email_send_skip
  + email_send
  + email_subscribe
  + email_unsubscribe       
  + list_users
  + lists
  + message_types
  + metadata
  + templates
  + users  
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

```
$ pip install tap-iterable
```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

```json
{
  "api_key": "xx",
  "start_date" : "2018-02-22T02:06:58.147Z",
  "api_window_in_days" : "10"
}
```

4. Run the Tap in Discovery Mode

    tap-iterable -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-iterable -c config.json --catalog catalog-file.json

## Development

First, clone this repo. Then, in the directory:

```
$ python -m venv tap-iterable
$ make dev
```

---

Copyright &copy; 2019 Stitch
