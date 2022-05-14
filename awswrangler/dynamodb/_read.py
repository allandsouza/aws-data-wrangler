"""Amazon DynamoDB Read Module (PRIVATE)."""

import logging
from typing import Optional

import boto3
import pandas as pd

from awswrangler._config import apply_configs

from ._utils import get_table

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def scan_table(
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Get a Pandas DataFrame with dynamodb items.

    Parameters
    ----------
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame filled by dynamodb items.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_dynamodb_table = wr.dynamodb.scan_table(table_name='table')

    """
    _logger.debug("Reading data from DynamoDB table")

    dynamodb_table = get_table(table_name=table_name, boto3_session=boto3_session)

    table_rows = dynamodb_table.scan()['Items']

    return pd.DataFrame(table_rows)
