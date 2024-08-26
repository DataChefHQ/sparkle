"""This module contains the models for the Sparkle package."""

import os
from enum import Enum, StrEnum
from typing import Any, TYPE_CHECKING
from collections.abc import Callable
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from sparkle.data_reader import DataReader


class Environment(StrEnum):
    """The environment of the application."""

    PRODUCTION = "production"
    DEVELOPMENT = "development"
    TESTING = "testing"
    ACCEPTANCE = "acceptance"
    LOCAL = "local"

    @classmethod
    def from_system_env(cls) -> "Environment":
        """Get the environment from the system environment variable."""
        env = os.environ.get("ENVIRONMENT", cls.LOCAL)
        return cls(env)

    @classmethod
    def all(cls) -> list["Environment"]:
        """Return all the environments."""
        return [env for env in cls]

    @classmethod
    def all_values(cls) -> list[str]:
        """Return all the environment values."""
        return [env.value for env in cls]


class TriggerMethod(Enum):
    """Tigger method types."""

    PROCESS = "process"
    REPROCESS = "reprocess"
    OPTIMIZE = "optimize"

    @classmethod
    def all(cls) -> list["TriggerMethod"]:
        """Return all the trigger methods."""
        return [e for e in cls]

    @classmethod
    def all_values(cls) -> list[str]:
        """Return all the trigger method values."""
        return [e.value for e in cls]


class InputField:
    """A class to define the input fields of a pipeline."""

    def __init__(self, name: str, type: type["DataReader"], **options: Any) -> None:
        """Initialize the input field with the given name, type, and options."""
        self.name = name
        self.type = type
        self.options = options


@dataclass
class Pipeline:
    """A class to define a pipeline."""

    name: str
    func: Callable
    method: TriggerMethod
    description: str | None = None
    inputs: list[InputField] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class Trigger:
    """A class to define a trigger request."""

    method: TriggerMethod
    pipeline_name: str
    environment: Environment
    options: dict[str, Any]
