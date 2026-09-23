"""Explicit component registry.

Registration is deliberately local and reviewable. A future plugin discovery
mechanism can replace this registry without changing application services.
"""

from dataclasses import dataclass, field
from typing import Callable


Factory = Callable[..., object]


@dataclass
class ComponentRegistry:
    sources: dict[str, Factory] = field(default_factory=dict)
    encoders: dict[str, Factory] = field(default_factory=dict)
    stores: dict[str, Factory] = field(default_factory=dict)
    publishers: dict[str, Factory] = field(default_factory=dict)

    def register(self, category: str, name: str, factory: Factory) -> None:
        values = getattr(self, category)
        if name in values:
            raise ValueError(f"Component already registered: {category}.{name}")
        values[name] = factory

    def create(self, category: str, name: str, *args, **kwargs):
        values = getattr(self, category)
        try:
            factory = values[name]
        except KeyError:
            available = ", ".join(sorted(values)) or "none"
            raise ValueError(
                f"Unsupported {category[:-1]} {name!r}; available: {available}"
            ) from None
        return factory(*args, **kwargs)


COMPONENTS = ComponentRegistry()
