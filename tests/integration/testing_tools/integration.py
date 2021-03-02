# pylint: disable=invalid-name
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
)
from unittest.mock import patch
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from itertools import chain
from singer.messages import (
    ActivateVersionMessage,
    RecordMessage,
    SchemaMessage,
    StateMessage,
    parse_message,
)
from .base import BaseTestCase, write_artifact
from .utils import dict_subset

if TYPE_CHECKING:
    from pathlib import Path
    from singer.messages import Message
    from .base import TapArgs


class MessageElement(NamedTuple):
    """A `Message` instance with an associated `index` representing its output
    index.
    """

    index: int
    message: "Message"


class StreamMessageTracker:
    """ Tracks messages for a particular stream. """

    def __init__(self, tap_stream_id: str):
        self.tap_stream_id = tap_stream_id
        self._messages: Dict[str, List[MessageElement]] = {
            "schemas": [],
            "records": [],
            "activate_versions": [],
        }

    @property
    def schema_messages(self):
        """ Returns tracked SCHEMA messages. """

        return self._messages["schemas"]

    @property
    def record_messages(self):
        """ Returns tracked RECORD messages. """

        return self._messages["records"]

    @property
    def activate_version_messages(self):
        """ Returns tracked ACTIVATE_VERSION messages. """

        return self._messages["activate_versions"]

    @property
    def messages(
        self,
    ) -> List[Union[RecordMessage, SchemaMessage, ActivateVersionMessage]]:
        """ Returns an unordered list of tracked messages. """

        return (
            self.schema_messages + self.record_messages + self.activate_version_messages
        )

    @property
    def summary(self) -> str:
        """ Returns a summary of tracked messages. """

        return f"""
        {self.tap_stream_id} had {len(self.messages)} messages output:
            - {len(self.schema_messages)} schema messages
            - {len(self.record_messages)} record messages
            - {len(self.activate_version_messages)} activate version messages
        """

    @property
    def sorted_messages(
        self,
    ) -> List[Union[RecordMessage, SchemaMessage, ActivateVersionMessage]]:
        """ Returns an ordered list of tracked messages. """

        return sorted(self.messages, key=lambda ele: ele.index)

    def add_message(
        self,
        message: Union[RecordMessage, SchemaMessage, ActivateVersionMessage],
        index: int,
    ) -> None:
        """ Adds a `message` to be tracked. """

        key = None

        if isinstance(message, RecordMessage):
            key = "records"
        elif isinstance(message, SchemaMessage):
            key = "schemas"
        elif isinstance(message, ActivateVersionMessage):
            key = "activate_versions"

        if key is None:
            raise TypeError(
                "`message` must be an instance of `RecordMessage`, `SchemaMessage`, or `ActivateVersionMessage`"
            )

        self._messages[key].append(MessageElement(index, message))


class OutputTracker:
    """ Tracks information regarding overall output. """

    def __init__(self):
        self.streams: Dict[str, StreamMessageTracker] = {}
        self.state_messages: List[StateMessage] = []
        self.index = -1

    @property
    def messages(self) -> chain:
        """ Aggregated stream messages and state messages into an `itertools.chain` instance """

        messages = chain(
            *[stream_tracker.messages for stream_tracker in self.streams.values()],
            self.state_messages,
        )

        return messages

    @property
    def sorted_messages(self) -> chain:
        """ Aggregated stream messages and state messages into a sorted `itertools.chain` instance """

        messages = chain(
            sorted(
                self.messages,
                key=lambda ele: ele.index,
            )
        )

        return messages

    @property
    def summary(self) -> str:
        """ Returns a summary of overall tracked message output. """

        total_schemas = total_records = total_activate_version = 0
        summaries = ""

        for stream_tracker in self.streams.values():
            total_schemas += len(stream_tracker.schema_messages)
            total_records += len(stream_tracker.record_messages)
            total_activate_version += len(stream_tracker.activate_version_messages)
            summaries += stream_tracker.summary

        output = f"""
        The output contained {self.index + 1} messages for {len(self.streams.keys())} stream(s):
            - {total_schemas} schema messages
            - {total_records} record messages
            - {total_activate_version} activate version messages
            - {len(self.state_messages)} state messages

        Streams:
        {summaries}
        """

        return output

    def add_stream_message_tracker(self, tap_stream_id: str) -> StreamMessageTracker:
        stream_tracker = StreamMessageTracker(tap_stream_id)

        self.streams[tap_stream_id] = stream_tracker

        return stream_tracker

    def add_message(self, message: "Message") -> None:
        """ Adds a messages to be tracked. """

        if isinstance(message, StateMessage):
            self.state_messages.append(MessageElement(self.index, message))

            self.index += 1

        if hasattr(message, "stream"):
            tap_stream_id = message.stream

            if tap_stream_id not in self.streams:
                stream_tracker = self.add_stream_message_tracker(tap_stream_id)
            else:
                stream_tracker = self.streams[tap_stream_id]

            stream_tracker.add_message(message, self.index)

            self.index += 1

    def add_messages(self, messages: Iterable["Message"]) -> None:
        for message in messages:
            self.add_message(message)

    def filter_messages(
        self,
        message_type: Optional[
            Union[
                Type[RecordMessage],
                Type[StateMessage],
                Type[ActivateVersionMessage],
                Type[SchemaMessage],
            ]
        ] = None,
        tap_stream_id: Optional[str] = None,
    ) -> chain:
        """ Filters all tracked messages as `MessageElement`s. """

        # May want to make more efficient, if any performance issues
        # are encountered. Regardless, could use some refactoring
        # eventually. May also want to allow for multiple `message_type`s.
        search_attr = {
            RecordMessage: "record_messages",
            ActivateVersionMessage: "activate_version_messages",
            SchemaMessage: "schema_messages",
            StateMessage: "state_messages",
            None: "messages",
        }[message_type]

        filtered_iters = []

        if tap_stream_id is not None:
            if message_type is StateMessage:
                raise ValueError(
                    "Cannot filter by `StateMessage`s and `tap_stream_id`."
                )

            filtered_iters.append(getattr(self.streams[tap_stream_id], search_attr))
        elif message_type is None:
            return self.messages
        elif message_type is not StateMessage:
            for stream_tracker in self.streams.values():
                filtered_iters.append(getattr(stream_tracker, search_attr))
        else:
            filtered_iters.append(self.state_messages)

        return chain(*filtered_iters)


class TapExecutor:
    """Handles tap execution

    Notice: This requires monkeypatching a few things, so use carefully.
    """

    def __init__(
        self,
        tap_entrypoint: Callable,
        tap_args: "TapArgs",
        artifacts_dir: Optional["Path"] = None,
    ):
        self.tap_entrypoint = tap_entrypoint
        self.artifacts_dir = artifacts_dir
        self.output = OutputTracker()

        # Set what would be CLI args
        self.parse_args_patcher = patch(f"{tap_entrypoint.__module__}.parse_args")
        self.mocked_pargse_args = self.parse_args_patcher.start()
        self.mocked_pargse_args.return_value = tap_args

        self._stdout = StringIO()
        self._stderr = StringIO()

    def run(self, write_artifacts=False) -> None:
        """ Executes the tap """

        with redirect_stderr(self._stderr), redirect_stdout(self._stdout):
            try:
                self.tap_entrypoint()
            except Exception as err:
                self.write_artifacts()

                raise err

        if write_artifacts:
            self.write_artifacts()

        for line in self._stdout.getvalue().split("\n"):
            # ignore blank lines, but nothing else
            # we want an exception raised
            if len(line.strip()) == 0:
                continue

            try:
                self.output.add_message(parse_message(line))
            except Exception as err:
                raise ValueError(f"Singer failed to parse message: {line}") from err

    def stop(self) -> None:
        """ Stops `singer.utils.parse_args` patcher """
        self.parse_args_patcher.stop()

    def write_artifacts(self) -> None:
        """ Writes tap's stdout and stderr to files within `TapExecutor.artifacts_dir`. """

        write_artifact(
            self._stdout.getvalue(), suffix=".tap-stdout", file_dir=self.artifacts_dir
        )
        write_artifact(
            self._stderr.getvalue(), suffix=".tap-stderr", file_dir=self.artifacts_dir
        )

    def __enter__(self) -> "TapExecutor":
        return self  # pragma: no cover

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.stop()


# pylint: disable=abstract-method
class TapIntegrationTestCase(BaseTestCase):
    """ A TestCase that provides common tap-related test assertions, and tap integration """

    def assertStreamInOutput(self, tap_stream_id: str):
        """ Ensure the tap outputs a message related to `tap_stream_id`. """
        self.assertIn(
            tap_stream_id,
            self.tap_executor.output.streams,
            msg=f'No output captured for stream "{tap_stream_id}". Captured streams: {self.tap_executor.output.streams.keys()}',
        )

    def assertStreamNotInOutput(self, tap_stream_id: str):
        """ Ensure the tap DOES NOT output a message related to `tap_stream_id`. """
        self.assertNotIn(
            tap_stream_id,
            self.tap_executor.output.streams,
            msg=f'Output captured for stream "{tap_stream_id}". Captured streams: {self.tap_executor.output.streams.keys()}',
        )

    def assertMessageCountEqual(
        self,
        expected_count: int,
        tap_stream_id: Optional[str] = None,
    ):
        """ Ensure total message count equals `expected_count`. """

        fail_msg = "Expected {} tap messages, only found {}"

        if tap_stream_id is None:
            total_messages = len(list(self.tap_executor.output.messages))
            fail_msg = fail_msg.format(expected_count, total_messages)
        else:
            if tap_stream_id not in self.tap_executor.output.streams:
                self.fail(f'Stream "{tap_stream_id}" is not tracked.')

            total_messages = len(
                self.tap_executor.output.streams[tap_stream_id].messages
            )
            fail_msg = (fail_msg + " for stream '{}'").format(
                expected_count, total_messages, tap_stream_id
            )

        self.assertEqual(
            total_messages,
            expected_count,
            msg=fail_msg,
        )

    def assertMessagesIncludes(
        self, search_message: "Message", ignored_keys: List[str] = None
    ):
        """ Ensure output messages include `message`. """

        tap_stream_id = getattr(search_message, "stream", None)
        args = {"message_type": type(search_message)}

        if tap_stream_id:
            args["tap_stream_id"] = tap_stream_id

        found = False
        for message_ele in self.tap_executor.output.filter_messages(**args):  # type: ignore
            message = message_ele.message

            if not isinstance(message, type(search_message)):
                continue

            # Ignore time_extracted, without mutating or serializing
            if (
                isinstance(message, RecordMessage)
                and message.stream == search_message.stream
                and dict_subset(message.record, ignored_keys)
                == dict_subset(search_message.record, ignored_keys)
                and message.version == search_message.version
            ):
                found = True
                break

            if message == search_message:
                found = True
                break

        if not found:
            self.fail(f'Could not find "{search_message} in tracked output.')

    def assertMessagesIncludesAll(
        self, messages: Iterable["Message"], ignored_keys: List[str] = None
    ):
        """ Ensure output messages include all `messages`. """

        for message in messages:
            self.assertMessagesIncludes(message, ignored_keys)

    def assertRecordMessagesFollowSchema(self):
        """Ensure SCHEMA messages are output prior to their records.

        While Singer's spec allows for this, in practice our records are never
        schemaless.
        """

        for stream_tracker in self.tap_executor.output.streams.values():
            found_schema = False

            for message_ele in stream_tracker.messages:
                message = message_ele.message

                if isinstance(message, SchemaMessage):
                    found_schema = True

                    continue

                if isinstance(message, RecordMessage):
                    if not found_schema:
                        self.fail(
                            f"{stream_tracker.tap_stream_id} outputs a RECORD message prior to a SCHEMA message."
                        )

                    break

    def assertStreamVersion(
        self,
        tap_stream_id: str,
        version: Optional[int] = None,
    ):
        """ Ensure ACTIVATE_VERSION message and RECORD messages output with `version`. """

        stream_output = self.tap_executor.output.streams[tap_stream_id]

        for message_ele in stream_output.messages:
            message = message_ele.message

            if isinstance(message, (RecordMessage, ActivateVersionMessage)):
                self.assertEqual(
                    message.version,
                    version,
                    msg=f"{type(message)}'s version is {message.version}', not {version} as expected",
                )

    def assertLatestBookmarksEqual(self, expected_bookmarks: Dict[str, str]):
        """ Ensure latest bookmarks equal `expected_bookmarks`. """

        latest_state_message = self.tap_executor.output.state_messages[-1].message
        bookmarks = latest_state_message.value.get("bookmarks", {})

        self.assertDictEqual(bookmarks, expected_bookmarks)


__all__ = [
    "TapExecutor",
    "TapIntegrationTestCase",
    "OutputTracker",
    "StreamMessageTracker",
    "BaseTestCase",
]
