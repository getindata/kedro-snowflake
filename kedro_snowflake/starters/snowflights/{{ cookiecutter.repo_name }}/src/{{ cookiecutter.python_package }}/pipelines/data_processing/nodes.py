from typing import Optional

import pandas as pd
import snowflake.snowpark as sp
from snowflake.snowpark.functions import udf


def _is_true(x: pd.Series) -> pd.Series:
    return x == "t"


def _parse_money(x: pd.Series) -> pd.Series:
    x = x.str.replace("$", "", regex=True).str.replace(",", "")
    x = x.astype(float)
    return x


def export_data_to_snowflake(companies: pd.DataFrame) -> sp.DataFrame:
    """Exports data to Snowflake.
    This node is only for the demonstration purposes
    """
    companies.columns = companies.columns.str.upper()
    return sp.context.get_active_session().create_dataframe(companies)


def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for companies.

    Args:
        companies: Raw data.
    Returns:
        Preprocessed data, with `company_rating` converted to a float and
        `iata_approved` converted to boolean.
    """

    @udf(
        name="parse_percentage",
        is_permanent=False,
        replace=True,
    )
    def parse_percentage(x: str) -> Optional[float]:
        return float(x.replace("%", "")) / 100.0 if x else None

    df = companies.withColumn("IATA_APPROVED", companies["IATA_APPROVED"] == "t")
    df = df.withColumn("COMPANY_RATING", parse_percentage("COMPANY_RATING"))
    return df


def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for shuttles.

    Args:
        shuttles: Raw data.
    Returns:
        Preprocessed data, with `price` converted to a float and `d_check_complete`,
        `moon_clearance_complete` converted to boolean.
    """
    shuttles["d_check_complete"] = _is_true(shuttles["d_check_complete"])
    shuttles["moon_clearance_complete"] = _is_true(shuttles["moon_clearance_complete"])
    shuttles["price"] = _parse_money(shuttles["price"])
    return shuttles


def create_model_input_table(
    shuttles: pd.DataFrame, companies: pd.DataFrame, reviews: pd.DataFrame
) -> pd.DataFrame:
    """Combines all data to create a model input table.

    Args:
        shuttles: Preprocessed data for shuttles.
        companies: Preprocessed data for companies.
        reviews: Raw data for reviews.
    Returns:
        Model input table.

    """
    companies = companies.toPandas()
    companies.columns = companies.columns.str.lower()
    rated_shuttles = shuttles.merge(reviews, left_on="id", right_on="shuttle_id")
    model_input_table = rated_shuttles.merge(
        companies, left_on="company_id", right_on="id"
    )
    model_input_table = model_input_table.dropna()
    return model_input_table
