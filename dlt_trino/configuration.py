from typing import ClassVar, List, Any, Dict, Final

from dlt import TSecretValue
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration

import dataclasses


@configspec(init=False)
class TrinoCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="trino", init=False, repr=False,  # type: ignore
                                               compare=False)
    database: str = None
    username: str = None
    password: TSecretValue = None
    host: str = None
    port: int = 5432
    connect_timeout: int = 15

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query


@configspec
class TrinoClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="trino", init=False, repr=False,  # type: ignore[misc]
                                                     compare=False)
    credentials: TrinoCredentials = None
