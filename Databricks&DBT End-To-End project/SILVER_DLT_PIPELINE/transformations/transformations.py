import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================
# BOOKINGS
# ============================================

@dlt.table(name="stage_bookings")
def stage_bookings():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/bookings/data")
    )


@dlt.view(name="trans_bookings")
def trans_bookings():
    df = dlt.read_stream("stage_bookings")
    return (
        df.withColumn("amount", col("amount").cast(DoubleType()))
          .withColumn("modifiedDate", current_timestamp())
          .withColumn("booking_date", to_date(col("booking_date")))
          .drop("_rescued_data")
    )


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}


@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    return dlt.read_stream("trans_bookings")


# ============================================
# FLIGHTS
# ============================================

@dlt.view(name="trans_flights")
def trans_flights():
    df = (
        spark.readStream.format("delta")
        .load("/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/flights/data")
    )
    return (
        df.drop("_rescued_data")
          .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type="1"
)


# ============================================
# PASSENGERS
# ============================================

@dlt.view(name="trans_passegers")
def trans_passengers():
    df = (
        spark.readStream.format("delta")
        .load("/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/customers/data")
    )
    return (
        df.drop("_rescued_data")
          .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passegers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type="1"
)


# ============================================
# AIRPORTS
# ============================================

@dlt.view(name="trans_airports")
def trans_airports():
    df = (
        spark.readStream.format("delta")
        .load("/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/airports/data")
    )
    return (
        df.drop("_rescued_data")
          .withColumn("modifiedDate", current_timestamp())
    )


dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type="1"
)


# ============================================
# Silver Business table
# ============================================

@dlt.table(name="silver_buisness")
def silver_buisness():
    b = dlt.read_stream("silver_bookings").drop("modifiedDate")

    # CDC targets can emit updates; read them with ignoreChanges/skipChangeCommits
    f = (spark.readStream
         .option("ignoreChanges", "true")
         .option("skipChangeCommits", "true")
         .table("silver_flights")
         .drop("modifiedDate"))

    p = (spark.readStream
         .option("ignoreChanges", "true")
         .option("skipChangeCommits", "true")
         .table("silver_passengers")
         .drop("modifiedDate"))

    a = (spark.readStream
         .option("ignoreChanges", "true")
         .option("skipChangeCommits", "true")
         .table("silver_airports")
         .drop("modifiedDate"))

    return (
        b.join(f, ["flight_id"])
         .join(p, ["passenger_id"])
         .join(a, ["airport_id"])
    )
