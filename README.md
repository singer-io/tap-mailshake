# tap-mailshake

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [MAILSHAKE API](https://api-docs.mailshake.com/)
- Extracts the following resources:
  - Campaigns
  - Recipients
  - Leads
  - Senders
  - Team Members
  - Activities
    - Clicks
    - Opens
    - Replies
    - Sent Messages
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

**campaigns**
- Endpoint: https://api.mailshake.com/2017-04-01/campaigns/list
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: INCREMENTAL
- Transformations: none

**recipients**
- Endpoint: https://api.mailshake.com/2017-04-01/recipients/list
- Primary key fields: id
- Foreign key fields: campaigns > id
- Replication strategy: INCREMENTAL (Query filtered)
    - Filter: campaignID (parent)
- Transformations: none

**leads**
- Endpoint: https://api.mailshake.com/2017-04-01/leads/list
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: INCREMENTAL
- Transformations: none

**senders**
- Endpoint: https://api.mailshake.com/2017-04-01/senders/list
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: INCREMENTAL
- Transformations: none

**team_members**
- Endpoint: https://api.mailshake.com/2017-04-01/team/list-members
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: INCREMENTAL
- Transformations: none

**sent_messages**
- Endpoint: https://api.mailshake.com/2017-04-01/activity/sent
- Primary key fields: id
- Foreign key fields: 
    - recipients > id
    - campaigns > id
- Replication strategy: INCREMENTAL
- Transformations: none

**opens**
- Endpoint: https://api.mailshake.com/2017-04-01/activity/opens
- Primary key fields: id
- Foreign key fields: 
    - recipients > id
    - campaigns > id
- Replication strategy: INCREMENTAL
- Transformations: none

**clicks**
- Endpoint: https://api.mailshake.com/2017-04-01/activity/clicks
- Primary key fields: id
- Foreign key fields: 
    - recipients > id
    - campaigns > id
- Replication strategy: INCREMENTAL
- Transformations: none

**replies**
- Endpoint: https://api.mailshake.com/2017-04-01/activity/replies
- Primary key fields: id
- Foreign key fields: 
    - recipients > id
    - campaigns > id
- Replication strategy: INCREMENTAL
- Transformations: none


## Authentication
This tap uses [simple authentication](https://api-docs.mailshake.com/#Simple) as defined by the Mailshake API docs.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-mailshake
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `api_key` is your Mailshake account's specific API key from the `Extensions>API` selection in the Mailshake UI.

    ```json
    {
        "api_key": "YourApiKeyFromMailshake",
        "domain": "api.mailshake.com",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-mailshake <api_user_email@your_company.com>"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "campaigns",
        "bookmarks": {
            "campaigns": "2019-09-27T22:34:39.000000Z",
            "recipients": "2019-09-28T15:30:26.000000Z",
            "leads": "2019-09-28T18:23:53Z",
            "senders": "2019-09-27T22:40:30.000000Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-mailshake --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_code/bytecode/StitchOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-mailshake --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-mailshake --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-mailshake --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the mailshake tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_mailshake -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    ************* Module tap_mailshake.sync
    tap_mailshake/sync.py:153:4: R1702: Too many nested blocks (7/5) (too-many-nested-blocks)
    tap_mailshake/sync.py:113:0: R0915: Too many statements (69/50) (too-many-statements)
    
    ------------------------------------------------------------------
    Your code has been rated at 9.95/10 (previous run: 9.95/10, +0.00)
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-mailshake --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    Checking stdin for valid Singer-formatted data
    The output is valid.
    It contained 33101 messages for 9 streams.
    
         11 schema messages
      33066 record messages
         24 state messages
    
    Details by stream:
    +---------------+---------+---------+
    | stream        | records | schemas |
    +---------------+---------+---------+
    | clicks        | 1341    | 1       |
    | team_members  | 7       | 1       |
    | senders       | 5       | 1       |
    | recipients    | 6203    | 3       |
    | campaigns     | 31      | 1       |
    | opens         | 13642   | 1       |
    | leads         | 812     | 1       |
    | sent_messages | 9207    | 1       |
    | replies       | 1818    | 1       |
    +---------------+---------+---------+
    ```
---

Copyright &copy; 2020 Stitch
