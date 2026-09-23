"""Composition root for the executable ingestion application."""

from .composition import RuntimeComponents, assemble, register_builtin_components
from .registry import COMPONENTS, ComponentRegistry

__all__ = [
    "COMPONENTS", "ComponentRegistry", "RuntimeComponents", "assemble",
    "register_builtin_components",
]
