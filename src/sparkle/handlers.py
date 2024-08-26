"""Handlers for the SparkETL application."""

import argparse
import logging
from typing import Protocol
from functools import cache

from .models import Trigger, Environment, TriggerMethod


logger = logging.getLogger(__name__)


class Application(Protocol):
    """The application interface to run triggers."""

    def run(self, trigger: Trigger) -> None:
        """Run the trigger with the given trigger."""
        ...


class TriggerHandler(Protocol):
    """The trigger handler interface to listen for triggers."""

    def __init__(self, application: Application) -> None:
        """Initialize the trigger handler with the given application."""
        ...

    def listen(self) -> None:
        """Listen for triggers and run the application."""
        ...


class ArgParsTriggerHandler(TriggerHandler):
    """The trigger handler to listen for triggers from command line arguments."""

    def __init__(self, application: Application) -> None:
        """Initialize the trigger handler with the given application."""
        self.application = application

    @property
    @cache
    def parsed_args(self) -> argparse.Namespace:
        """Parse the command line arguments and return the parsed arguments."""
        parser = argparse.ArgumentParser(
            description="Triggers pipelines of the SparkETL application.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--env",
            help="The environment to run the application in.",
            default=Environment.from_system_env().value,
            choices=Environment.all_values(),
            type=str,
        )
        parser.add_argument(
            "--pipeline-name",
            help="Name of the pipeline to be triggered.",
            required=True,
            type=str,
        )
        parser.add_argument(
            "--trigger-method",
            help="The trigger method of the pipeline.",
            choices=TriggerMethod.all_values(),
            required=True,
            type=str,
        )
        parser.add_argument(
            "--options",
            metavar="KEY=VALUE",
            nargs="+",
            help="Set a number of key-value pairs as trigger options (do not put spaces before or after the = sign). If a value contains spaces, you should define it with double quotes: 'foo=\"this is a sentence\". Note that ' values are always treated as strings.",
        )

        args, unknown = parser.parse_known_args()
        logger.warning(f"Unknown arguments: {unknown}")

        return args

    def listen(self) -> None:
        """Listen for triggers from the command line arguments and run the application."""
        self.application.run(
            Trigger(
                pipeline_name=self.parsed_args.pipeline_name,
                environment=Environment(self.parsed_args.env),
                method=TriggerMethod(self.parsed_args.trigger_method),
                options={
                    "delete_before_reprocess": self.parsed_args.not_delete_before_reprocess,
                    **(
                        {
                            k: v
                            for k, v in (
                                arg.split("=") for arg in self.parsed_args.options
                            )
                        }
                        if self.parsed_args.options
                        else {}
                    ),
                },
            )
        )
