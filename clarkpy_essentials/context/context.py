from dataclasses import dataclass
from .. import DataCatalog


class Context:

    def __init__(self, **kwargs) -> None:

        self._catalog_fields = []

        for key, value in kwargs.items():
            setattr(self, key, value)
            self._catalog_fields.append(key)


    def _check_kwargs(self, key, value):
        if key == 'catalog':
            assert isinstance(value, DataCatalog), 'Key catalog must be a DataCatalog instance'

    @property
    def catalog_fields(self):
        return self._catalog_fields

    def __repr__(self) -> str:
        pass