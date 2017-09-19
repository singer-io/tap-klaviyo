# tap-klaviyo

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Klaviyo API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Klaviyo metrics API](https://www.klaviyo.com/docs/api/metrics)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state for incremental endpoints
- Updates full tables for global exclusions and lists endpoints

## Quick start

1. Install

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install tap-klaviyo
    ```

2. Create the config file

    Create a JSON file containing your API key, start date, email and events.
    The Klaviyo events you want to replicate each have a unique metric ID that needs
    to be added to the config file.
    Use -l option to list all available Klaviyo metrics in your account.

    ```json
    {
        "credentials": {
            "api_key": "pk_XYZ"
        },
        "start_date": "2017-01-01T00:00:00Z",
        "user_agent": "email_address",
    }
    ```

3. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the metrics endpoints to force the application to only fetch events since those dates. If you omit the file it will fetch all
    commits and issues.

    ```json
    {"bookmarks":
        "receive": {"since": "2017-04-01T00:00:00Z"},
        "open": {"since": "2017-04-01T00:00:00Z"},
    }
    ```

4. [Optional] Run discover command and save catalog into catalog file

    ```bash
    tap-klaviyo --config config.json --discover
    ```

5. Run the application

    `tap-klaviyo` can be run with:

    ```bash
    tap-klaviyo --config config.json [--state state.json] [--catalog catalog.json]
    ```

---


Copyright &copy; 2017 Stitch
