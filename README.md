# tap-klaviyo

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Klaviyo API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Klaviyo metrics API](https://www.klaviyo.com/docs/api/metrics)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install tap-klaviyo
    ```

3. Create the config file

    Create a JSON file containing your API key, start date, email and events.
    The Klaviyo events you want to replicate each have a unique metric ID that needs
    to be added to the config file.
    Use -d option to list all available Klaviyo metrics in your account.

    ```json
    {
        "api_key": "asasdkldasdlkaasdkfdsjadf",
        "start_date": "2017-01-01T00:00:00Z",
        "user_agent": "email_address",
        "events": [
          {"name": "receive", "id": "ID", "primary_key": "uuid"},
          {"name": "click", "id": "ID", "primary_key": "uuid"},
          {"name": "open", "id": "ID", "primary_key": "uuid"},
          {"name": "bounce", "id": "ID", "primary_key": "uuid"},
          {"name": "unsubscribe", "id": "ID", "primary_key": "uuid"},
          {"name": "mark_as_spam", "id": "ID", "primary_key": "uuid"}
        ]
    }
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the metrics endpoints to force the application to only fetch events since those dates. If you omit the file it will fetch all
    commits and issues.

    ```json
    {"receive": "2017-01-01T00:00:00Z",
      "open": "2017-01-01T00:00:00Z",
      "click": "2017-01-01T00:00:00Z",
      "unsubscribe": "2017-01-01T00:00:00Z",
      "bounce": "2017-01-01T00:00:00Z",
      "mark_as_spam": "2017-01-01T00:00:00Z"
    }

    ```

5. Identify the event Ids.

    Use the -d option to list out all the events that are available in the Klaviyo account.
    The top level id (EVENTID) below, is the ID that needs to be added to the config file


    ```bash
    tap-klaviyo --config config.json -d discover
    ```
    ```json
    {
      "updated": "2017-01-01 00:00:00",
      "name": "eventA",
      "created": "2017-01-01 00:00:00",
      "object": "metric",
      "id": "EVENTID",
      "integration": {
        "category": "Internal",
        "object": "integration",
        "id": "internalID",
        "name": "Klaviyo"
      }
    },
```


5. Run the application

    `tap-klaviyo` can be run with:

    ```bash
    tap-klaviyo --config config.json [--state state.json]
    ```

---
