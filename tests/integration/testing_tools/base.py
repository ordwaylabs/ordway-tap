from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union
from unittest import TestCase
from datetime import datetime, timezone
from inspect import getfile
from json import dumps as json_dumps
from json import loads as json_loads
from os import environ
from os import name as SYS_NAME
from os.path import dirname, isfile, join
from pathlib import Path
from singer.catalog import Catalog
from singer.utils import load_json, strftime
from vcr import VCR
from .mask import mask_values

if TYPE_CHECKING:
    from .integration import TapExecutor
    from .mask import Format, StrFormat


class TapArgs:
    """Represents tap arguments

    Note: `*_path` attrs will be None if a Dict is passed in.
    """

    def __init__(
        self,
        config: Union[Dict[str, Any], Path],
        state: Union[None, Dict[str, Any], Path] = None,
        catalog: Union[None, Dict[str, Any], Catalog, Path] = None,
        discover: bool = False,
        **kwargs,
    ):
        self.catalog_path = self.state_path = self.config_path = None

        if isinstance(catalog, Path):
            self.catalog_path = str(catalog)
            catalog = Catalog.load(catalog)
        elif isinstance(catalog, dict):
            catalog = Catalog.from_dict(catalog)

        if isinstance(config, Path):
            self.config_path = str(config)
            config = load_json(config)
        if isinstance(state, Path):
            self.state_path = state
            state = load_json(state)

        self.config = config
        self.state = state
        self.catalog = catalog
        self.discover = discover

        for name, val in kwargs.items():
            setattr(self, name, val)


def write_artifact(
    output: str, suffix: str = "", file_dir: Optional[Path] = None
) -> None:
    """ Helper function for writing a file. """

    now = strftime(datetime.now(timezone.utc))

    if SYS_NAME == "nt":
        now = now.replace("-", "").replace(":", "")

    file_path = Path(now) if file_dir is None else file_dir / Path(now)
    file_path = file_path.with_suffix(suffix)

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w") as artifact_fp:
        artifact_fp.write(output)


# Modified from vcrpy-unittest
class VCRMixin:
    """ A TestCase mixin that provides VCR integration on a class-basis. """

    vcr_enabled = True

    def setUp(self):
        if self.vcr_enabled and self.cassette.all_played:
            self.cassette.rewind()

    @classmethod
    def setUpClass(cls):
        if cls.vcr_enabled:
            kwargs = cls._get_vcr_kwargs()
            myvcr = cls._get_vcr(**kwargs)

            cls.cassette_exists = isfile(
                join(myvcr.cassette_library_dir, cls._get_cassette_name())
            )
            cls.cassette_cm = myvcr.use_cassette(cls._get_cassette_name())
            cls.cassette = cls.cassette_cm.__enter__()

    @classmethod
    def tearDownClass(cls):
        if cls.vcr_enabled:
            cls.cassette_cm.__exit__()

    @classmethod
    def _get_vcr(cls, **kwargs) -> VCR:
        if "cassette_library_dir" not in kwargs:
            kwargs["cassette_library_dir"] = cls._get_cassette_library_dir()

        return VCR(**kwargs)

    @classmethod
    def _get_vcr_kwargs(cls, **kwargs):
        return kwargs

    @classmethod
    def _get_cassette_library_dir(cls) -> str:
        testdir = dirname(getfile(cls))

        return join(testdir, "cassettes")

    @classmethod
    def _get_cassette_name(cls) -> str:
        return f"{cls.__name__}.yaml"


def _default_response_mutator(
    json,
    masked_fields: Optional[Dict[str, Union["StrFormat", "Format"]]] = None,
    masked_formats: Optional[List["StrFormat"]] = None,
) -> None:
    if isinstance(json, list):
        for i, val in enumerate(json):
            json[i] = mask_values(
                val,
                masked_fields=masked_fields,
                masked_formats=masked_formats,
            )
    if isinstance(json, dict):
        mask_values(
            json,
            masked_fields=masked_fields,
            masked_formats=masked_formats,
        )


class BaseTestCase(VCRMixin, TestCase):
    """ Base tap test case """

    MASKED_RESPONSE_FIELDS: Optional[Dict[str, Union["StrFormat", "Format"]]] = None
    MASKED_RESPONSE_FORMATS: Optional[List["StrFormat"]] = None
    RESPONSE_MUTATORS: Tuple[
        Callable[[Union[List, Dict]], Union[List, Dict]], ...
    ] = tuple([])

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.tap_executor = cls.get_tap_executor()
        cls.tap_executor.run(
            write_artifacts=environ.get("TEST_WRITE_TAP_ARTIFACTS") == "1"
        )

    @classmethod
    def tearDownClass(cls):
        cls.tap_executor.stop()

        super().tearDownClass()

    @classmethod
    def _get_vcr(cls, **kwargs) -> "VCR":
        vcr = super()._get_vcr(**kwargs)

        vcr.decode_compressed_response = True
        vcr.filter_headers = ("authorization",)
        vcr.before_record_request = cls._before_record_request
        vcr.before_record_response = cls._before_record_response

        return vcr

    @classmethod
    def get_tap_executor(cls) -> "TapExecutor":
        raise NotImplementedError(
            "Must override `TapTestCase.get_tap_entrypoint` in subclass."
        )

    @classmethod
    def _before_record_response(cls, response):
        """ Allows pre-processing of cassette responses """

        if cls.cassette_exists:
            return response

        if (
            cls.MASKED_RESPONSE_FIELDS is None
            and cls.MASKED_RESPONSE_FORMATS is None
            and (cls.RESPONSE_MUTATORS is None or len(cls.RESPONSE_MUTATORS) == 0)
        ):
            return response

        json = json_loads(response["body"]["string"])

        _default_response_mutator(
            json, cls.MASKED_RESPONSE_FIELDS, cls.MASKED_RESPONSE_FORMATS
        )

        for response_mutator in cls.RESPONSE_MUTATORS:
            response_mutator(json)

        response["body"]["string"] = json_dumps(json).encode("utf-8")

        return response

    @classmethod
    def _before_record_request(cls, response):
        """ Allows pre-processing of cassette responses """

        return response


__all__ = ["TapArgs", "write_artifact", "BaseTestCase"]
