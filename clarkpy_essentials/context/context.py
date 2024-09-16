from dataclasses import dataclass


class Context:

    def __init__(self, **kwargs) -> None:

        self._catalog_fields = []

        for key, value in kwargs.items():
            setattr(self, key, value)
            self._catalog_fields.append(key)

    @property
    def catalog_fields(self):
        return self._catalog_fields

    def __repr__(self) -> str:
        pass