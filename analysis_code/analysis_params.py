import polars as pl

#db = 'nrel_hd' 
db = 'nrel_va_doe'
data_folder = "C:/Users/mbruchon/OneDrive - NREL/Documents/Projects/FUSE/"
out_path = data_folder + "out/"
old_outputs_path = "C:/Users/mbruchon/OneDrive - NREL/Documents/Projects/FUSE/old_query_results/"
project_in_path = data_folder + "Inputs/"
shared_in_path = "C:/Users/mbruchon/OneDrive - NREL/Documents/Shared_Data/"
mhdv_path = "C:/Users/mbruchon/OneDrive - NREL/Documents/Projects/EVI_MHDV/Data/"

fips = pl.scan_csv(shared_in_path + "fips_data.csv")

weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
weekday_numbers = pl.scan_csv(project_in_path + "weekday_numbers.csv")
class_adoption_levels = pl.scan_csv(project_in_path + "class_adoption_levels.csv", dtypes={"Class": pl.UInt32})
vehicle_archetypes = pl.scan_csv(project_in_path + "vehicle_archetypes.csv", dtypes={"Class": pl.UInt32})
expn_owner_types = pl.scan_csv(project_in_path + "expn_owner_types.csv")
naics = pl.scan_csv(project_in_path + "naics_2digit_text.csv")
parcels = (pl.scan_csv(project_in_path + "VA/parcels_202309061729.csv", dtypes={"fips_code": pl.Utf8})
    .select(["use_code_std_ctgr_desc_lps", "use_code_std_desc_lps", "fips_code", "site_zip", "_x_coord", "_y_coord"])
    .rename({"site_zip": "zipcode"})
)
domicile_dwell_share_by_vocation = (pl.scan_csv(project_in_path + "domicile_dwell_share_by_vocation.csv", dtypes={"Vocation": pl.Utf8})
    .filter(pl.col("Pctile_Range")=="25_75")
)
county_region_mappings = pl.scan_csv(project_in_path + "county_region_mappings_expn.csv")
expn_groupings = pl.scan_csv(project_in_path + "expn_groupings.csv").filter(pl.col(f'include_{db}')==1).select(["model_ty_tx","body_style_group_tx","mpv"])
expn_county_to_fips = pl.scan_csv(project_in_path + "expn_county_to_fips.csv", dtypes={"fips_code": pl.Utf8})
owner_type_to_land_use = pl.scan_csv(project_in_path + "owner_type_to_land_use.csv")
expn = (pl.scan_csv(project_in_path + "fuse_region_2bplus_expn.csv", dtypes={"Class": pl.Utf8})
    .with_columns(pl.col("Class").str.slice(0, 1).cast(pl.UInt32))
    .with_columns(pl.lit(db).alias("db"))
    .filter(pl.col("Class") < 7)
    .with_columns(pl.concat_str([(pl.col("naics1_lvl4_cd")//1000).cast(pl.Utf8),pl.lit("_"),(pl.col("naics2_lvl4_cd")//1000).cast(pl.Utf8)]).alias("both_naics"))
    .with_columns(pl.col("both_naics").str.split("_").list.first().alias("naics1"),
                  pl.col("both_naics").str.split("_").list.last().alias("naics2"))
    .with_columns(pl.col("naics1").fill_null("None"),
                  pl.col("naics2").fill_null("None"))
    .join(county_region_mappings, on=["db","state_abbr","county_name"], how="inner") #Add region labels and filter to correct database
    .join(expn_groupings, on=["model_ty_tx","body_style_group_tx"], how="inner") #Filter 
    .join(expn_county_to_fips, on="county_name", how="left")
)

expn_volumes_cluster = (pl.scan_csv(project_in_path + "expn_county_volumes_no_bus_mpv_8000up.csv")
    .filter(~pl.col("state_abbr").is_in(["AA","AE","GU","PR","VI"]))
    .filter(pl.col("county_name")!= "UNSPECIFIED")
    .join(pl.scan_csv(project_in_path + "expn_class_to_epa_groups.csv"), how="left", on="gross_wt_class")
    .join(pl.scan_csv(project_in_path + "expn_county_taf_crosswalk.csv"), how="left", on=["state_abbr","county_name"])
    .join(pl.scan_csv(project_in_path + "taf_cluster_crosswalk.csv"), how="left", left_on="taf", right_on="taf_multistate")
    .group_by(["cluster","class_group"])
        .agg(pl.col("sum").sum().alias("Vehicle_Count"))
)

vehicle_size_measures = pl.LazyFrame({"Measure": ["ConsideredVehicles", "EligibleVehicles", "OperatingVehicles", "OperatingVehicles"], 
              "Statistic": ["NumberOf", "NumberOf", "NumberOf", "NumberOf"]})