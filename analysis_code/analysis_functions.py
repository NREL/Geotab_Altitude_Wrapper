# Import libraries, define data zones and time ranges, and authenticate
import os
import sys
import polars as pl
import polars.selectors as cs
from datetime import datetime, timedelta
import numpy as np
from scipy.optimize import linprog
from scipy.stats import CensoredData, weibull_min

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from api_wrapper.user_params import *
from api_wrapper.altitude_params import *
import api_wrapper.altitude_functions as altitude

def getResults(metadata: pl.LazyFrame, 
               subzones: pl.LazyFrame, 
               results: pl.LazyFrame, 
               query_ids: pl.LazyFrame) -> pl.LazyFrame:
    return (results
        .join(query_ids, 
              how="semi", on="Query_ID")
        .join(metadata.join(query_ids, how="semi", on="Query_ID"), 
              how="inner", on="Query_ID")
        .join(subzones.join(query_ids, how="semi", on="Query_ID"), 
              how="inner", on=["Query_ID","Subzone"])
    )

def getResultsBy(df: pl.LazyFrame, 
                 grouping_to_keep: pl.LazyFrame, 
                 group_by_dimensions: pl.LazyFrame, 
                 measures_and_statistics: pl.LazyFrame = pl.LazyFrame({"Statistic": ["Total","NumberOf","Average","Median"]})) -> pl.LazyFrame:
    return (df
        .filter(pl.col("Group_By")==grouping_to_keep)
        .join(measures_and_statistics, how="semi", on=measures_and_statistics.columns)
        .with_columns(pl.concat_str([pl.col("Statistic"),pl.col("Measure")]).alias("MeasureStatistic"))
        .collect()
        .pivot(values="Value", index=group_by_dimensions, on="MeasureStatistic", aggregate_function=None, sort_columns=True)
        .lazy()
        .sort(group_by_dimensions)
    )

def getLatestQueryBy(metadata: pl.LazyFrame,
                     grouping_cols: list[str]) -> pl.LazyFrame:
    return (metadata
        .select(grouping_cols + ["Query_Processed_Time", "Query_ID"])
        .sort(grouping_cols + ["Query_Processed_Time", "Query_ID"], descending = [True for col in grouping_cols] + [True, True])
        .group_by(grouping_cols).agg(pl.col("Query_ID").first())
        .sort(grouping_cols)
    )

def getLatestQueryByAll(metadata: pl.LazyFrame) -> pl.LazyFrame:
    grouping_cols = metadata.select((~cs.by_name("Query_Processed_Time","Query_ID")) & (~cs.by_dtype(pl.List(pl.Utf8)))).columns
    return (metadata.pipe(getLatestQueryBy, grouping_cols)
    )

def getAllMetadata(path: str) -> pl.LazyFrame:
    query_filters = (pl.scan_parquet(path + "all_results.parquet")
        .pipe(recoverMetadataFromResults)
    )
    query_zone_counts = (pl.scan_parquet(path + "all_subzone_definitions.parquet")
        .pipe(summarizeSubzones)
    )
    return (pl.scan_parquet(path + "all_metadata.parquet")
        .join(query_filters, how="left", on="Query_ID")
        .pipe(parseVehicleClass)
        .join(query_zone_counts, how="left", on="Query_ID")
    )

def keepUniqueColumns(df: pl.DataFrame) -> pl.DataFrame:
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]
    df = df[[s.name for s in df if ((s.dtype==pl.List) or (s.n_unique() > 1))]]
    df = df[[s.name for s in df if not ((s.dtype==pl.List) and (s.list.join("|").n_unique()<2))]]
    return df

def recoverMetadataFromResults (results: pl.LazyFrame) -> pl.LazyFrame:
    filters = (results
        .filter(pl.col("Measure").str.contains("Filter"))
        .select("Query_ID", "Measure", "Value")
        .unique()
        .collect().pivot(
            index = "Query_ID",
            on = "Measure",
            values = "Value"
        ).lazy()
    )

    single_val_measures = (results
        .filter(pl.col("Group_By") == "None", 
                pl.col("Group") == "All", 
                pl.col("Statistic") == "",
                ~pl.col("Measure").str.contains("Filter"))
        .select("Query_ID", "Measure", "Value")
        .unique()
        .with_columns(pl.col("Value").n_unique().over("Query_ID", "Measure").alias("n_unique"))
        .filter(pl.col("n_unique") == 1, 
                ~pl.col("Measure").is_in(["JourneysPerDay","EligibleDomicileVehicleCount","DomicileDutyCycleCount"]))
        .select("Query_ID", "Measure", "Value")
        .unique()
        .collect().pivot(
            index = "Query_ID",
            on = "Measure",
            values = "Value"
        ).lazy()
    )

    single_group_groupbys = (results
        .filter(~pl.col("Group_By").is_in(["None","VehicleClass"]))
        .select("Query_ID", "Group_By", "Group")
        .unique()
        .with_columns(pl.col("Group").n_unique().over("Query_ID", "Group_By").alias("n_unique"))
        .filter(pl.col("n_unique")==1)
        .select("Query_ID", "Group_By", "Group")
        .unique()
        .collect().pivot(
            index = "Query_ID",
            on = "Group_By",
            values = "Group"
        ).lazy()
    )

    
    vehicle_class_list = (results
        .filter(pl.col("Group_By")=="VehicleClass")
        .select("Query_ID", "Group_By", "Group")
        .unique()
        .with_columns(pl.col("Group").n_unique().over("Query_ID", "Group_By").alias("n_unique"))
        .select("Query_ID", "Group_By", "Group")
        .unique()
        .group_by("Query_ID","Group_By").agg(pl.col("Group").unique())
        .collect().pivot(
            index = "Query_ID",
            on = "Group_By",
            values = "Group")
        .rename({"VehicleClass": "VehicleClassResults"})
        .with_columns(
            pl.col("VehicleClassResults").list.eval(pl.element().str.extract(r"([A-Za-z]+) \(").str.replace("Trucks", "Trucks")).list.drop_nulls().list.sort().alias("VehicleTypeResultsList"),
            pl.col("VehicleClassResults").list.eval(pl.element().str.extract(r"Class ([^\']+?)")).list.drop_nulls().list.sort().alias("VehicleClassResultsList")
        )
    ).lazy()

    all_queries = results.select(pl.col("Query_ID").unique())
   
    return (all_queries
        .join(filters, how="left", on="Query_ID")
        .join(single_val_measures, how="left", on="Query_ID")
        .join(single_group_groupbys, how="left", on="Query_ID")
        .join(vehicle_class_list, how="left", on="Query_ID")
    )

def parseVehicleClass(metadata: pl.LazyFrame) -> pl.LazyFrame:
    return (metadata
        .with_columns(
            pl.col("VehicleClassFilter").str.split("|").list.eval(pl.element().str.extract(r"VehicleType\': \'([^\']+)\'")).alias("VehicleTypeList"),
            pl.col("VehicleClassFilter").str.split("|").list.eval(pl.element().str.extract(r"\'WeightClass\': \'([^\']+)\'").str.replace("Class ", "")).alias("VehicleClassList")
        )
        .with_columns(pl.coalesce(pl.col("VehicleTypeList", "VehicleTypeResultsList")).alias("VehicleTypeList"),
                      pl.coalesce(pl.col("VehicleClassList", "VehicleClassResultsList")).alias("VehicleClassList"))
        .drop("VehicleClassResultsList", "VehicleTypeResultsList")
        .with_columns(
            pl.when(pl.col("VehicleTypeList").list.unique().list.len()>1)
                .then(pl.col("VehicleClassList").list.unique().list.sort().list.join("|"))
                .when(pl.col("VehicleTypeList").list.len()==0)
                .then(pl.lit(""))
                .otherwise(pl.col("VehicleTypeList").list.first())
            .alias("Type"),
            pl.when(pl.col("VehicleClassList").list.unique().list.len()>1)
                .then(pl.col("VehicleClassList").list.unique().list.sort().list.join("|"))
                .when(pl.col("VehicleClassList").list.len()==0)
                .then(pl.lit(""))
                .otherwise(pl.col("VehicleClassList").list.first())
            .alias("Class")
        )
    )

def summarizeSubzones(subzone_definitions: pl.LazyFrame) -> pl.LazyFrame:
    subzone_cols = ["RegionCategory", "RegionId", "Origin_ZoneId", "Destination_ZoneId", 
                        "LocationType", "SubTypeTag", "Destination_ZoneType","Origin_ZoneType","Destination_ZoneCategory","Origin_ZoneCategory",
                        "DayOfWeek", "Description", "ZonePairIdentifier"]
    subzone_cols = [col for col in subzone_cols if col in subzone_definitions.collect_schema()]
    return (subzone_definitions
        .group_by(pl.col("Query_ID"))
            .agg(pl.col(subzone_cols).explode().drop_nulls().unique())
        .with_columns(
            pl.col(subzone_cols).drop_nulls().list.len().name.suffix("_Count")
        )
        .with_columns(
            pl.when(pl.col(subzone_cols).drop_nulls().list.len() == 0)
                .then(pl.lit("None"))
                .when(pl.col(subzone_cols).drop_nulls().list.len() == 1)
                .then(pl.col(subzone_cols).drop_nulls().list.first())
                .otherwise(pl.lit("Multiple"))
            .name.suffix("_Value")
        )
    )

def addDayGroup(df) -> pl.DataFrame:
    return (df
        .with_columns(pl.when(pl.col("DayOfWeek").is_in(business_days)).then(pl.lit("Weekday"))
                .when(pl.col("DayOfWeek")=="All").then(pl.lit("All"))
                .otherwise(pl.lit("Weekend")).alias("DayGroup"))
    )

def addOperatingDaysPct(pctiles) -> pl.DataFrame:
    return (pctiles.with_columns(
        (pl.col("PercentileOperatingDays")/(pl.col("PercentileOperatingDays").max().over(["DateFrom","DayOfWeek"]))).alias("PercentileOperatingDaysPct")
    ))

def rollupOperatingDaysPct(pctiles, group_by_cols) -> pl.DataFrame:
    return (pctiles
        .filter(pl.col("Percentile") % 10.0 == 0.0)
        .group_by(group_by_cols)
            .agg(pl.col("PercentileOperatingDaysPct").mean().alias("Pct_Active"))
    )

def addWeights(rollup, group_vars) -> pl.DataFrame:
    return (rollup
        .with_columns((pl.col("operating_subgroup_cluster_day") / pl.col("operating_subgroup_cluster_day").sum().over(group_vars)).alias("weight"))
        .with_columns(pl.col("weight").fill_null(0.0))
)

def addPercentileWeights(df, multiplier=1.0) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("Percentile").is_in([0.0,5.0,20.0,25.0,70.0,75.0,90.0,95.0]))
            .then(1.0 * multiplier)
            .otherwise(2.0 * multiplier).cast(pl.Int64).alias("pctile_weighting"))

def addCensoringVarsWide(df, censored_vars, upper_tail_extender = None, percentile_groupings = ["DateFrom","Region","Vocation","VehicleClasses","DayOfWeek","DayGroup"]) -> pl.DataFrame:
    df = pl.concat([df,
        df.filter(pl.col("Percentile")==pl.col("Percentile").min()).with_columns(pl.lit(0.0).alias("Percentile"))
    ])
    df = df.sort(percentile_groupings + ["Percentile"])
    for censored_var in censored_vars:
        df = (df
            .with_columns(
                pl.col(f'Percentile{censored_var}').alias(f'{censored_var}_Left'),
                pl.col(f'Percentile{censored_var}').alias(f'{censored_var}_Right'))
            .with_columns(pl.when(pl.col("Percentile")==0.0).then(pl.lit(0.0))
                .otherwise(pl.col(f'{censored_var}_Left'))
                .alias(f'{censored_var}_Left'))
            .with_columns(pl.when(pl.col("Percentile")==0.0).then(pl.col(f'{censored_var}_Right'))
                .otherwise(pl.col(f'{censored_var}_Left').shift(-1).over(percentile_groupings))
                .alias(f'{censored_var}_Right'))
        )
        if upper_tail_extender is None:
            df = (df
                .with_columns(pl.when(pl.col("Percentile") != 95.0).then(pl.col(f'{censored_var}_Right'))
                    .otherwise(np.Inf)
                    .alias(f'{censored_var}_Right'))
            )
        else:
            df = (df
                .with_columns(pl.when(pl.col("Percentile") != 95.0).then(pl.col(f'{censored_var}_Right'))
                    .otherwise(pl.col(f'{censored_var}_Left') + upper_tail_extender * ((pl.col(f'{censored_var}_Right')-pl.col(f'{censored_var}_Left')).shift(1).over(percentile_groupings)))
                    .alias(f'{censored_var}_Right'))
            )

    return df

def addCensoringVarsLong(lf: pl.LazyFrame, repetitions: int = 1) -> pl.LazyFrame:
    df = pl.concat([df,
        df.filter(pl.col("Percentile")==pl.col("Percentile").min()).with_columns(pl.lit(0.0).alias("Percentile"),pl.lit(0.0).alias("Value"))
    ])
    sort_columns = df.select(pl.all().exclude("Percentile","Value")).columns
    return (df
        .sort(sort_columns + ["Percentile"])
        .with_columns(pl.col("Value").shift(-1).over(sort_columns).fill_null(np.Inf).alias("Value_Next"),
                      (pl.col("Percentile").shift(-1).over(sort_columns).fill_null(100.0) - pl.col("Percentile")).alias("Percentile_Weight"))
        .with_columns(pl.col("Value").repeat_by(pl.col("Percentile_Weight").mul(repetitions).cast(pl.UInt16)).alias("Lower"),
                    pl.col("Value_Next").repeat_by(pl.col("Percentile_Weight").mul(repetitions).cast(pl.UInt16)).alias("Upper"))
        .drop("Percentile","Percentile_Weight","Value","Value_Next")
        .group_by(pl.all().exclude("Lower","Upper"))
            .agg(pl.col("Lower","Upper").flatten())
    )

def getWeibullFit(lf: pl.LazyFrame, measure_names: list[str] = []) -> pl.LazyFrame:
    if len(measure_names) > 0:
        lf = lf.filter((pl.col("Measure").is_in(measure_names)))
    return(lf
        .pipe(addCensoringVarsLong)
        .with_columns(
            pl.struct(["Lower","Upper"]).map_elements(lambda x: weibull_min.fit(CensoredData.interval_censored(x["Lower"], x["Upper"]), loc=0), return_dtype=pl.List(pl.Float64)).alias("Weibull_Min")
        )
        .drop("Lower","Upper")
    )
