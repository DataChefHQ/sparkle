"""The core module of Sparkle."""

import logging
from typing import Any
from collections.abc import Callable
from pyspark.sql import SparkSession

from .models import Pipeline, Trigger, InputField, TriggerMethod
from .data_reader import DataReader
from .data_writer import DataWriter


logger = logging.getLogger(__name__)


class Sparkle:
    """The core class of Sparkle responsible for defining and running pipelines.

    ...

    Attributes:
    ----------
    reader : DataReader
        The data reader instance to read data from the source.
    writer : DataWriter
        The data writer instance to write data to the destination.
    _pipelines : dict[str, Pipeline]
        The dictionary to store the defined pipelines.

    Methods:
    -------
    add_pipeline_rule(pipeline_name, description, func, method, inputs, options)
        Add a trigger rule for the given pipeline.

    pipeline(name, inputs, method, **options)
        A decorator to define an processing job for the given pipeline.

    run(trigger, session)
        Process a trigger request with the given Spark session
    """

    def __init__(self, reader: DataReader, writer: DataWriter) -> None:
        """Initialize the Sparkle instance with the given reader and writer."""
        self.reader = reader
        self.writer = writer
        self._pipelines: dict[str, Pipeline] = {}

    def add_pipeline_rule(
        self,
        pipeline_name: str,
        description: str | None,
        func: Callable,
        method: TriggerMethod,
        inputs: list[InputField],
        options: dict[str, Any],
    ):
        """Add a trigger rule for the given pipeline."""
        if pipeline_name in self._pipelines.keys():
            raise RuntimeError(f"Pipeline `{pipeline_name}` is already defined.")
        else:
            self._pipelines[pipeline_name] = Pipeline(
                name=pipeline_name,
                description=description,
                inputs=inputs,
                func=func,
                method=method,
                options=options,
            )

    def pipeline(
        self,
        name: str,
        inputs: list[InputField],
        method: TriggerMethod = TriggerMethod.PROCESS,
        **options,
    ) -> Callable:
        """A decorator to define an processing job for the given pipeline."""

        def decorator(func):
            self.add_pipeline_rule(
                pipeline_name=name,
                description=func.__doc__,
                func=func,
                inputs=inputs,
                method=method,
                options=options,
            )

            return func

        return decorator

    def run(self, trigger: Trigger, session: SparkSession) -> None:
        """Process a trigger request with the given Spark session."""
        logger.info(
            f"Pipeline `{trigger.pipeline_name}` is triggered with `{trigger.method}` method."
        )

        if requested_pipeline := self._pipelines.get(trigger.pipeline_name):
            dataframes = self.reader.read(requested_pipeline.inputs, session)
            requested_pipeline.func(trigger, **dataframes)
        else:
            raise NotImplementedError(
                f"Pipeline `{trigger.pipeline_name}` is not defined."
            )
