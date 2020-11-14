from typing import TYPE_CHECKING, Tuple, Optional, Dict, Any
from pathlib import Path
from os import environ
from os.path import dirname, join, abspath
from ordway_tap import main
from ordway_tap.api.consts import DEFAULT_API_VERSION
from .testing_tools import TapArgs, TapExecutor, TapIntegrationTestCase
from .testing_tools.utils import deselect_all_streams_except
from .testing_tools.mask import StrFormat

if TYPE_CHECKING:
    from vcr import VCR
    from .testing_tools.mask import Format

TEST_FIXTURES_DIR = abspath(join(dirname(__file__), "..", "fixtures"))
DEFAULT_START_DATE = "2020-08-01T00:00:00Z"

DEFAULT_MASKED_RESPONSE_FIELDS = {
    "quickbooks_id": StrFormat.RANDOM,
    "salesforce_id": StrFormat.RANDOM,
    "sf_account_id": StrFormat.RANDOM,
    "crexi_user_sales_id": StrFormat.RANDOM,
    "crexi_account_global_id": StrFormat.RANDOM,
    "crexi_customer_type": StrFormat.RANDOM,
    "price_book_id": StrFormat.RANDOM,
    "first_name": StrFormat.RANDOM,
    "display_name": StrFormat.RANDOM,
    "last_name": StrFormat.RANDOM,
    "phone": StrFormat.PHONE,
    "mobile": StrFormat.PHONE,
    "address1": StrFormat.RANDOM,
    "address2": StrFormat.RANDOM,
    "city": StrFormat.RANDOM,
    "state": StrFormat.RANDOM,
    "zip": StrFormat.RANDOM,
    "county": StrFormat.RANDOM,
    "country": StrFormat.RANDOM,
    "updated_by": StrFormat.EMAIL,
    "created_by": StrFormat.EMAIL,
    "name": StrFormat.RANDOM,
    "notes": StrFormat.RANDOM,
    "job_title": StrFormat.RANDOM,
    "department": StrFormat.RANDOM,
    "description": StrFormat.RANDOM,
    "website": StrFormat.RANDOM,
    "other_email": StrFormat.EMAIL,
}


def get_tap_config():
    staging = environ.get("TEST_CONFIG_STAGING")

    if staging is None:
        staging = True
    else:
        staging = staging == "1"

    return {
        "api_credentials": {
            "x_company": environ.get("TEST_CONFIG_X_COMPANY", "TEST_COMPANY"),
            "x_user_email": environ.get(
                "TEST_CONFIG_X_USER_EMAIL", "TEST_USER_EMAIL@example.com"
            ),
            "x_user_token": environ.get("TEST_CONFIG_X_USER_TOKEN", "TEST_USER_TOKEN"),
            "x_api_key": environ.get("TEST_CONFIG_X_API_KEY", "TEST_API_KEY"),
        },
        "api_version": environ.get("TEST_CONFIG_API_VERSION", DEFAULT_API_VERSION),
        "staging": staging,
        "start_date": DEFAULT_START_DATE,
    }


class BaseOrdwayTestCase(TapIntegrationTestCase):
    SELECTED_STREAMS: Tuple[str, ...] = tuple([])

    START_DATE: str = DEFAULT_START_DATE
    STATE: Optional[Dict[str, Any]] = None

    @classmethod
    def _get_vcr(cls, **kwargs) -> "VCR":
        vcr = super()._get_vcr(**kwargs)
        vcr.filter_headers = (
            "X-User-Token",
            "X-User-Email",
            "X-API-KEY",
            "X-User-Company",
            "X-Company-Token",
            "__cfduid",
            "Set-Cookie",
        )

        return vcr

    @classmethod
    def get_tap_executor(cls):
        if len(cls.SELECTED_STREAMS) == 0:
            raise ValueError("Must select at least one stream for a test case.")
        if cls.START_DATE is None:
            raise ValueError("Expected a ISO-8601 START_DATE.")

        config = get_tap_config()
        config["start_date"] = cls.START_DATE

        args = TapArgs(
            config=config,
            catalog=Path(TEST_FIXTURES_DIR, "catalog.json"),
            state={} if cls.STATE is None else cls.STATE,
        )

        deselect_all_streams_except(args.catalog, cls.SELECTED_STREAMS)

        return TapExecutor(tap_entrypoint=main, tap_args=args)
