from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from inspect import isabstract
from typing import Any, AsyncGenerator

from fastapi import APIRouter, Depends, FastAPI


class LifespanDependency(ABC):
    """
    All lifespan dependencies must inherit from this class.
    Typical usage scenario for such dependencies is to
    initialize a resource and make it available to the application
    during the lifespan of the application (and through the request lifecycle).
    """

    _registry: list[type[LifespanDependency]] = []

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if not isabstract(cls) and cls not in LifespanDependency._registry:
            LifespanDependency._registry.append(cls)

    @staticmethod
    @abstractmethod
    def __call__(*args: Any, **kwargs: Any) -> Any:
        """
        This method is called
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        yield

    def get_routers(self) -> list[APIRouter]:
        """Override to contribute routers to the application.
        Please note that all routers will be included in the root APIRouter under api prefix.
        """
        return []

    @classmethod
    def depends(cls) -> Any:
        return Depends(cls.__call__)
