from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict
import json
 
def get_df(spark: SparkSession, tokens: Dict) -> DataFrame:
    """
    Flatten an XML node's <ID> array (with @type + text) into columns.

    Parameters
    ----------
    df : DataFrame
    xml_col : str                     # column that holds XML text
    node_path : str                   # dot path to the node that directly contains <ID ...>
                                      # e.g. "Location_Usage_Reference"
                                      #      "Usage_Data.Type_Data.Type_Reference"
    keys : dict                       # {"WID":"wid", "Location_Usage_ID":"location_usage_id"}
    carry_cols : list[str] | None     # columns to carry through (business keys)

    Returns
    -------
    DataFrame with columns: carry_cols + values(keys)
    """

    reader_options = {
        "readChangeFeed": "true"
    }

    df = spark.readStream.options(**reader_options).table(tokens["df"])
    xml_col = tokens["xml_col"]
    node_path = tokens["node_path"]
    keys = json.loads(tokens["keys"].replace("'",'"'))
    carry_cols = json.loads(tokens["carry_cols"].replace("'",'"')) or []

    # --- Build quoted DDL for from_xml ---
    # End shape we want at target node:
    #   `ID`: ARRAY<STRUCT<`_VALUE`: STRING, `type`: STRING>>
    tags = [t.strip() for t in node_path.split(".") if t.strip()]
    inner = "ARRAY<STRUCT<`_VALUE`: STRING, `type`: STRING>>"
    inner = f"STRUCT<`ID`: {inner}>"
    for t in reversed(tags):
        inner = f"STRUCT<`{t}`: {inner}>"
    schema = inner  # IMPORTANT: do NOT wrap again

    # --- Build a quoted reference to that ID array in the parsed column ---
    # _xml.`tag1`.`tag2`...`.ID`
    id_ref = "_xml" + "".join([f".`{t}`" for t in tags]) + ".`ID`"

   

    print("[flatten_xml_field] Using schema:", schema)
    print("[flatten_xml_field] ID array ref :", id_ref)

    # --- Parse and map ---
    parsed = (
        df.withColumn(
            "_xml",
            F.from_xml(
                F.col(xml_col),
                schema,
                {"attributePrefix": "", "valueTag": "_VALUE"}  # matches `_VALUE` in schema
            )
        ).withColumn(
            "_map",
            F.expr(
                f"map_from_entries(transform({id_ref}, x -> struct(x.`type` as key, x.`_VALUE` as value)))"
            )
        )
    )

    # --- Project carry cols + flattened keys ---
    selects = [F.col(c) for c in carry_cols]
    for k, outcol in keys.items():
        selects.append(F.col("_map")[F.lit(k)].alias(outcol))
    return parsed.select(*selects)