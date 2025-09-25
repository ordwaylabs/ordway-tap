# tap-ordway

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [OrdwayLabs](https://www.ordwaylabs.com/)
- Extracts the following resources:
  - billing_runs
  - billing_schedules
  - charges
  - chart_of_accounts
  - contacts
  - coupons
  - credits
  - customer_notes
  - customers
  - invoices
  - orders
  - payment_methods
  - payment_runs
  - payments
  - plans
  - products
  - refunds
  - revenue_rules
  - revenue_schedules
  - statements
  - subscriptions
  - usages
  - webhooks
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Installation
### Requirements
- Python 3.6+
- mkvirtualenv
- pip

### Steps
Run following commands on terminal from the project directory
```bash
python3 -m venv ~/.virtualenvs/tap-ordway
source ~/.virtualenvs/tap-ordway/bin/activate
pip install -e .
```

### To Run
`$ source ~/.virtualenvs/tap-ordway/bin/activate`

`$ tap-ordway -c config.json --catalog catalog.json -s state.json`


You can generate the catalog.json by following command:

`$ tap-ordway -c config.json --discover > catalog.json`

The sample config JSON is format is given below:
```json
{
"company": "Rocky",
"user_email": "me@example.com",
"user_token": "123usertoken",
"api_key": "123secret",
"start_date": "2019-12-01"
}
```

The `start_date` indicates the data at which the tap should start syncing data when no bookmark exists in the state for that particular stream.

The following configuration keys are optional:
- `staging` - Whether or not to use the staging environment (staging.ordwaylabs.com)
- `api_version` - Which Ordwaylabs API version to use (e.g. "v1")
- `api_url` - An alternative URL to which the API requests will be made (e.g. "https://localhost:3000/v1/"). When specified, it will take precendence over `staging` and `api_version`.
- `rate_limit_rps` - The amount of requests to allow per second (defaults to `null`, disabling rate limiting)

The State JSON should be passed by user.
The Tap will be printing the STATE message, the last state message should send when running next time.

Sample JSON:

```json
{
"currently_syncing": "credits",
"bookmarks": {
"credits": {
"updated_date": "2020-11-14T05:59:48.842000Z"
}
}
}
```

## Testing
1. Install the dev extra requirements
```bash
pip install .[dev]
```
2. Execute tox with the default environments: `py36`, `py37`, `py38`, and `type` (for static type checking via mypy)
```bash
tox
```

Additionally, you can generate a [Coverage](https://coverage.readthedocs.io/en/coverage-5.3/]) report by using the `coverage` environment:
```bash
tox -e coverage
```

For more information on tox, please refer to its [documentation](https://tox.readthedocs.io/en/latest/index.html).

### Testing with singer-check-tap

*singer-check-tap* is a tool for testing whether or not a tap adheres to the Singer specification. For more information, please review its [documentation](https://github.com/singer-io/singer-tools#singer-check-tap).

1. Install the [singer-tools](https://github.com/singer-io/singer-tools) package
```bash
pip install singer-tools
```
2. Execute singer-check-tap

```bash
singer-check-tap --tap tap-ordway --config config.json
```

In this mode, singer-check-tap will execute the tap itself, run it in discover mode to generate a catalog, perform a stateless run and a stateful run, and validate the tap's output.

If you need to test with a modified catalog, you can do so by piping the tap's output directly into singer-check-tap like so:

```bash
tap-ordway --config config.json --catalog catalog.json | singer-check-tap
```

---

Copyright &copy; 2020 Stitch
