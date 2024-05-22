# tap-klaviyo

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Klaviyo API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Klaviyo metrics API](https://developers.klaviyo.com/en/reference/api_overview)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state for incremental endpoints
- Updates full tables for global exclusions and lists endpoints

Singer taps function in two modes: [discovery mode](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md) and [sync mode](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md). Before running the tap in sync mode, you should run the tap in discovery mode and direct the output to a file called catalog.json, which can be used as an input to run the tap in sync mode and to specify which streams should be synced (see Step 4).

## Quick start

1. Install

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install tap-klaviyo
    ```

2. Create the config file

    Create a JSON file containing your API key, start date, email and request timeout (Optional parameter. Default value: 300 seconds).

    ```json
    {
        "api_key": "pk_XYZ",
        "start_date": "2017-01-01T00:00:00Z",
        "user_agent": "email_address",
        "request_timeout": 300
    }
    ```

3. Run discover command and save catalog into catalog file

    ```bash
    tap-klaviyo --config config.json --discover > catalog.json
    ```

4. Select streams to sync in catalog.json file
[This link](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md) explains the process of formatting catalog.json for stream selection (scroll to "Stream/Field Selection).

- Option 1: To select a stream to sync, add `{"breadcrumb": [], "metadata": {"selected": true}}` to its "metadata" entry.

- Option 2: Use an open-source tool called [Singer Discover](https://github.com/chrisgoddard/singer-discover) to format the catalog.json file.
    
    
5. [Optional] Create the initial state file

    You can provide a JSON file that contains a date for the metrics endpoints to force the application to only fetch events since those dates. If you omit the file it will fetch all
    commits and issues.

    ```json
    {"bookmarks":
        "receive": {"since": "2017-04-01T00:00:00Z"},
        "open": {"since": "2017-04-01T00:00:00Z"},
    }
    ```

6. Run the application

    `tap-klaviyo` can be run with:

    ```bash
    tap-klaviyo --config config.json [--state state.json] [--catalog catalog.json]
    ```

## Debugging

If you have made changes to your repository, you may need to run the following command after navigating to your local repository (before running the tap):

    pip install -e .


---

Copyright &copy; 2017 Stitch
