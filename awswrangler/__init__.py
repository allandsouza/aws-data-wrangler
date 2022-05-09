"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import importlib.util
import logging as _logging

from awswrangler import (  # noqa
    athena,
    catalog,
    chime,
    cloudwatch,
    data_api,
    dynamodb,
    emr,
    exceptions,
    lakeformation,
    mysql,
    neptune,
    opensearch,
    postgresql,
    quicksight,
    redshift,
    s3,
    secretsmanager,
    sqlserver,
    sts,
    timestream,
)
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler._config import config  # noqa
from awswrangler._distributed import _initialize_ray

if importlib.util.find_spec("ray"):
    _initialize_ray()

__all__ = [
    "athena",
    "catalog",
    "chime",
    "cloudwatch",
    "emr",
    "data_api",
    "dynamodb",
    "exceptions",
    "opensearch",
    "quicksight",
    "s3",
    "sts",
    "redshift",
    "lakeformation",
    "mysql",
    "neptune",
    "postgresql",
    "secretsmanager",
    "sqlserver",
    "config",
    "timestream",
    "__description__",
    "__license__",
    "__title__",
    "__version__",
]


_logging.getLogger("awswrangler").addHandler(_logging.NullHandler())
