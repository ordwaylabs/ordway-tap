# ordway-tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [OrdwayLabs](https://www.ordwaylabs.com/)
- Extracts the following resources:
  - billing_schedules
  - credits
  - customers
  - invoices
  - orders
  - payments
  - products
  - refunds
  - revenue_schedules
  - subscriptions
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

### Installation
##### Requirements:
- Python 3.6+
- mkvirtualenv
- pip

##### Steps:
Run following commands on terminal from the project directory

`$ python3 -m venv ~/.virtualenvs/ordway-tap`

`$ source ~/.virtualenvs/ordway-tap/bin/activate`

`$ pip install -e .`

##### To Run
`$ source ~/.virtualenvs/ordway-tap/bin/activate`

`$ ordway-tap -c config.json --catalog catalog.json -s state.json`


You can generate the catalog.json by following command:

`$ ordway-tap -c config.json --discover > catalog.json`

The sample config JSON is format is given below:
```
{
  "api_credentials": {
    "x_company": "X-Company",
    "x_company_token": "API Token",
    "x_user_email": "User Email",
    "x_user_token": "User Token",
    "x_api_key": "API Key",
    "endpoint": "Endpoint URL"
  }
}
```

The State JSON should be passed by user. 
The Tap will be printing the STATE message, the last state message should send when running next time. 

Sample JSON:

```
{
	"credits": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:38.589"
	},
	"invoices": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:38.835"
	},
	"orders": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:39.026"
	},
	"customers": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:39.275"
	},
	"revenue_schedules": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:39.523"
	},
	"billing_schedules": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:41.218"
	},
	"subscriptions": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:42.578"
	},
	"payments": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:43.018"
	},
	"products": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:43.813"
	},
	"refunds": {
		"synced": true,
		"last_synced": "2020-10-01T15:38:44.141"
	}
}

```
---

Copyright &copy; 2020 Stitch
