# Define functions and classes for interacting with the API
import time
import glob
import pickle
import json
import os
import sys
import datetime
import polars as pl
import numpy as np
import geopandas as gpd
from shapely.geometry import shape
from geojson import dumps
import mygeotab as gt
from datetime import datetime, timezone
from ratelimit import limits, sleep_and_retry
from . import altitude_params
from . import user_params

class QueryResult:
    def __init__(self, 
                 params: dict,
                 query_info: pl.DataFrame,
                 subzone_info: pl.DataFrame,
                 results: pl.DataFrame,
                 json: list
    ):
        self.params = params
        self.query_info = query_info
        self.subzone_info = subzone_info
        self.results = results
        self.json = json

rate_limits_10min = {
    "getStopAnalytics": 3,
    "getRegionalDomicileAnalytics": 5,
    "getOriginDestinationMatrix": 3,
    "getRouteAnalysis": 5,
    "getOdEdgeAnalysis": 5,
    "getSpeedAnalysisPerSegment": 5,
    "getZonesById": 10,
    "createZone": 40
}
rate_limit_10min_default = 20
rate_limit_10min_smallest = min(rate_limits_10min.values())

rate_limits_60min = {
    "getStopAnalytics": 5,
    "getOriginDestinationMatrix": 5,
    "getRegionalDomicileAnalytics": 30,
    "getRouteAnalysis": 30,
    "getSpeedAnalysisPerSegment": 30,
    "getZonesById": 30,
    "createZone": 100
}
rate_limit_60min_default = 100
rate_limit_60min_smallest = min(rate_limits_60min.values())

summary_metrics = ["Average","Avg","Median","Med","Mdn","Stdev","Count","NumberOf","Minimum","Min","Maximum","Max","Percent","Pct"]

subzoneDescriptorsAll = ["Subzone","DayOfWeek","RegionId","Description","ZoneDescription","ISO_3166_2",
                         "RegionCategory","LocationType","SubTypeTag","ZoneType","ZoneSubType","ZoneId",
    "ZonePairIdentifier", "IsPurchased"
    "Origin_ZoneId", "Origin_PassThrough", "Origin_ISO_3166_2", "Origin_ZoneType", "Origin_ZoneCategory", "Origin_Description",
    "Destination_ZoneId", "Destination_PassThrough", "Destination_ISO_3166_2", "Destination_ZoneType", "Destination_ZoneCategory", "Destination_Description",
    "IsOneWay", "OsmLandUse","AsOfDate","ReferenceDate", "Geography",
    "OriginZoneId", "OriginPassThrough", "ZonePairId", "DestinationZoneDescription", "OriginZoneType", 
    "OriginZoneDescription", "DestinationZoneType", "DestinationZoneId", "DestinationPassThrough"]

query_info_fields = ["Query_ID","JobKey", "id","status","totalRows","error","VehicleClassSchemeId","DistanceThresholdFilter","PercentileThresholdFilter",
                     "DomicileTripDistanceFilter","DomicileStopDurationFilter","DomicileStopsFilter",
                     "MinimumDomicileTripDistanceFilter","MinimumDomicileStopDurationFilter","MinimumDomicileStopsFilter","IsMetric", "DateRange",
                     "DateRanges","TimeRange","TimeRanges","DaysOfWeek", "AggregationDateRanges",
                     "VehicleClasses", "VocationIds","componentTimeRange"
                     "VehicleClassFilter","VocationFilter","NAICSFilter","FuelTypesFilter",
                     "TripChainDuration", "stopDurationBins", "NAICS", "vocations","vehicleClassSchemeId","vehicleClasses","includeGeography","vocationIds", ]

service_names = {
    "getDatabaseInfo": "dna-altitude-general",
    "getExpansionFactors": "dna-altitude-general",
    "getIndustries": "dna-altitude-general",
    "getVehicleClasses": "dna-altitude-general",
    "getOriginDestinationMatrix": "dna-altitude-od",
    "getRegions": "dna-altitude-od",
    "getZones": "dna-altitude-od",
    "getZonesByRadius": "dna-altitude-od",
    "getContainedZones": "dna-altitude-zones",
    "getCustomZones": "dna-altitude-zones",
    "getCustomZoneTypes": "dna-altitude-zones",
    "getZonesData": "dna-altitude-zones",
    "createCustomZone": "dna-altitude-zones",
    "createCustomZoneType": "dna-altitude-zones",
    "updateCustomZoneType": "dna-altitude-zones",
    "updateCustomZones": "dna-altitude-zones",
    "getDailyRegionalDomicileAnalytics": "dna-altitude-stop-analytics",
    "getRegionalDomicileAnalytics": "dna-altitude-stop-analytics",
    "getStopAnalytics": "dna-altitude-stop-analytics",
}
function_names = {
    "createZoneType": "getData",
    "getDatabaseInfo": "getData",
    "getIndustries": "getData",
    "getRegions": "getData",
    "getVehicleClasses": "getData",
}
group_name_indices = {'DayOfWeek': 0,
 'FuelType': 0,
 'Hour': 0,
 'Industry': 0,
 'Month': 0,
 'Vocation': 1,
 'VehicleClass': 3
}
group_name_labels = {'DayOfWeek': 'DayOfWeek',
 'FuelType': 'FuelType',
 'Hour': 'Hour',
 'Industry': 'NAICS_Code_1',
 'Month': 'Month',
 'Vocation': 'Vocation',
 'VehicleClass': 'VehicleClass'
}

def get_client(db):
    return gt.API(username=user_params.accounts[db]['email'], 
                password=user_params.accounts[db]['pw'], 
                server='altitude.geotab.com',
                database=user_params.accounts[db]['db'],
                timeout=3600)
                #cert='C:/Users/mbruchon/Downloads/_.geotab.com.pem')
                #cert='C:/Users/mbruchon/AppData/Roaming/.certifi/cacert.pem')

def get_mygeotab_bq_data(client, service_name, function_name, function_parameters):
    results = client.call("GetAltitudeData",
                          serviceName=service_name,
                          functionName=function_name,
                          functionParameters=function_parameters)
    return results

def check_for_errors(results):
    errors = results.get('errors',[])
    if (len(errors) == 0):
        if (results['apiResult'].get('errors') is not None):
            errors = results['apiResult'].get('errors',[])
            if (len(errors) == 0):
                if (results['apiResult'].get('results') is not None) & (isinstance(results['apiResult'].get('results'), dict)):
                    errors  = results['apiResult']['results'].get('errors',[])
    return errors

@sleep_and_retry
@limits(rate_limit_10min_smallest, 10*60+2*60)
@sleep_and_retry
@limits(rate_limit_60min_smallest, 60*60+2*60)
def create_bigquery_job(
        client,
        function_parameters,
        service_name = "dna-altitude-stop-analytics", 
        function_name = "createQueryJob"):
    error = "create_job_error"
    try:
        results = get_mygeotab_bq_data(client,
                                       service_name=service_name,
                                       function_name=function_name,
                                       function_parameters=function_parameters
                                       )
        errors = check_for_errors(results)
        if len(errors) > 0:
            raise Exception()
        
        if len(results["apiResult"]["results"]) == 1:
            job = results["apiResult"]["results"][0]
        else:
            job = results["apiResult"]["results"]
        return job
    except:
        errors = check_for_errors(results)
        print({"error": error,"details":errors})
        return errors
    
def wait_for_bigquery_job_to_complete(
        client,
        params,
        service_name = "dna-altitude-stop-analytics", 
        function_name = "getJobStatus"):
    error = "job_status_error"
    try:
        results = get_mygeotab_bq_data(client,
                                       service_name=service_name,
                                       function_name=function_name,
                                       function_parameters=params,                                       
                                       )
        
        errors = check_for_errors(results)
        if len(errors) > 0:
            raise Exception()
        
        job = results['apiResult']["results"][0]
        if job and job["status"] and job["status"]["state"] != "DONE":
            time.sleep(1)
            return wait_for_bigquery_job_to_complete(client, params, service_name, function_name)

        return job

    except:
        errors = check_for_errors(results)
        print({"error": error,"details":errors})
        return errors


def fetch_bigquery_data(client,
                        function_parameters,
                        service_name="dna-altitude-stop-analytics",
                        function_name="getQueryResults",
                        ):
    index = 1
    error = "fetching_data_error"
    
    while index:
        try:
            results = get_mygeotab_bq_data(client,
                                           function_parameters=function_parameters,
                                           service_name=service_name,
                                           function_name=function_name,
                                           )['apiResult']

            error = results["results"][0].get("error", None)
            rows = results["results"][0].get("rows", None)
            page_token = results["results"][0].get("pageToken", None)
            total_rows = results["results"][0].get("totalRows", None)

            function_parameters["pageToken"] = page_token
            yield {"data": [rows, total_rows, index], "error": error}
            index += 1
            if not page_token:
                index = None
                yield 

        except:
            print({"error": error,"results":results})
            index = None
            yield error

def check_bigquery_job_status(
        index,
        client,
        params,
        service_name):
    results = get_mygeotab_bq_data(client,
                                    service_name=service_name,
                                    function_name="getJobStatus",
                                    function_parameters=params,                                       
                                    )
    errors = check_for_errors(results)
    if len(errors) > 0:
        error_string = f'Error on job {index+1}: {errors}'
        return error_string
    else:
        job = results['apiResult']['results'][0]
        return job
    
def get_function_and_service_name(query_type: str):
    function_name = function_names.get(query_type, "createQueryJob")
    service_name = ''
    if query_type in service_names:
        service_name = service_names[query_type]
    else:
        service_name = f'Error: cannot run query type {query_type}. Add its service_name to the service_names dictionary in altitude_functions.py.'
        print(service_name)
    return function_name, service_name

def get_finished_job_results(client, job, params, service_name):
    data = [job]
    job_iterator = fetch_bigquery_data(client, params, service_name)  
    # loop through the results one page at a time and append to results array
    for data_page in job_iterator:
        page = [] if data_page == None else data_page["data"][0]
        error = None if data_page == None else data_page.get("error", None)
        if (error):
            print(error)
            break

        data.extend(page)

    return data

def get_params_in_folder(path, drop_id = True) -> list[dict]:
    query_params = []
    for params_file in sorted(glob.glob(path + '*.params')):
        filehandler = open(params_file, 'rb')
        params = pickle.load(filehandler)
        filehandler.close()
        if drop_id == True:
            del params['jobId']
            del params['pageToken']
        found = False
        for existing_param in query_params:
            if str(params) == str(existing_param):
                found = True
                break
        if not found:
            query_params.append(params)
    return query_params
    
def get_results_in_folder(path) -> list[dict]:
    query_results = []
    for params_file in sorted(glob.glob(path + '*.result')):
        filehandler = open(params_file, 'rb')
        results = pickle.load(filehandler)
        filehandler.close()
        query_results.append(results)
    return query_results

def process_finished_jobs(client, query_params_list, job_ids, job_statuses, job_outputs, out_path, serialize, parse):
    submitted_indices = [idx for idx in range(len(job_statuses)) if job_statuses[idx] == "submitted"]
    for submitted_index in submitted_indices:
        function_name, service_name = get_function_and_service_name(query_params_list[submitted_index]["queryType"])
        job = check_bigquery_job_status(submitted_index, client, query_params_list[submitted_index], service_name=service_name)
        if isinstance(job, str):
            print(job)
            if "Error" in job:
                job_statuses[submitted_index] = job
        elif job:
            job_ids[submitted_index] = job['id']
            query_params_list[submitted_index]['jobId'] = job['id']
            if job['status'] and job['status']['state'] == "DONE":
                job_statuses[submitted_index] = "executed"
                job_outputs[submitted_index] = get_finished_job_results(client, job, query_params_list[submitted_index], service_name=service_name)
                if serialize:
                    if out_path is not None:
                        filehandler = open(str(out_path + job['id'] + ".result"), 'wb') 
                        pickle.dump(job_outputs[submitted_index], filehandler)
                        filehandler.close()
                        filehandler = open(str(out_path + job['id'] + ".params"), 'wb') 
                        pickle.dump(query_params_list[submitted_index], filehandler)
                        filehandler.close()
                    else:
                        print("Could not serialize un-parsed results because out_path was not specified!")
                if parse:
                    job_outputs[submitted_index] = parse_query_output(job_outputs[submitted_index], query_params_list[submitted_index], out_path, serialize)
                    job_statuses[submitted_index] = "processed"
                    print(f'Job {submitted_index+1} completed and processed.')
                else:
                    print(f'Job {submitted_index+1} completed.')
    return query_params_list, job_statuses, job_outputs

#TODO: implement for queries that are left in submitted state
def handle_failed_queries(out_path):
    this_query_id = '' 
    combined = ''
    if os.path.isfile(out_path + 'submitted_query_ids.csv'):
        pl.read_csv
        filehandler = open('submitted_queries.pkl', 'rb')
        submitted_query_ids = pickle.load(filehandler)
        filehandler.close()
    else:
        filehandler = open(str(out_path + this_query_id + ".result"), 'wb') 
        pickle.dump(combined.json, filehandler)
        filehandler.close()

    filehandler = open(str(out_path + this_query_id + ".params"), 'wb') 
    pickle.dump(combined.params, filehandler)
    filehandler.close()

def query_api(client, query_params_list, out_path = None, serialize=True, parse=True):
    if (out_path is None) and serialize:
        print("A problem occurred: serialization was requested, but no out_path was specified.")
        return None, None, None
    #Handle case of one set of parameters being passed in, rather than a list
    if isinstance(query_params_list, dict):
        query_params_list = [query_params_list]

    job_ids = [None] * len(query_params_list)
    job_statuses = ["not submitted"] * len(query_params_list)
    job_outputs = [None] * len(query_params_list)
    # Create all jobs
    for i in range(0,len(query_params_list)):
        function_name, service_name = get_function_and_service_name(query_params_list[i]["queryType"])
        if "Error" in service_name:
            job_statuses[i] = service_name
            continue

        job = create_bigquery_job(client,
                            query_params_list[i], 
                            service_name=service_name, 
                            function_name=function_name)
        print(f'Job {i+1} of {len(query_params_list)} created.')
        if function_name == "getData": 
            job_statuses[i] = "returned"
            job_outputs[i] = job
            print(f'Job {i+1} returned.')
        elif not isinstance(job, dict):
            job_statuses[i] = "returned"
            job_outputs[i] = job
            print(f'Job {i+1} returned.')
        elif job.get('id') is None: 
            job_statuses[i] = "returned"
            job_outputs[i] = job
            print(f'Job {i+1} returned.')
        else:
            job_statuses[i] = "submitted"
            job_ids[i] = job['id']
            query_params_list[i]['jobId'] = job['id']

        if serialize or parse: 
            process_finished_jobs(client, query_params_list, job_ids, job_statuses, job_outputs, out_path, serialize, parse)

    # Poll job statuses
    while(sum([1 for status in job_statuses if status == "submitted"]) > 0):
        query_params_list, job_statuses, job_outputs = process_finished_jobs(client, query_params_list, job_ids, job_statuses, job_outputs, out_path, serialize, parse)
        time.sleep(1)

    return job_ids, job_statuses, job_outputs

def parse_query_output(result, params, out_path = None, serialize = True):
    if len(result) == 1: 
        print("Result had no content (length==1). Not saving the result.")
        return None
    
    def add_col(df: pl.DataFrame, value, name: str) -> pl.DataFrame:
        return (df
            .with_columns(pl.lit(value).alias(name))
        )
    def add_series_col(df: pl.DataFrame, values, name: str) -> pl.DataFrame:
        return (df
            .with_column(pl.Series(name = name, values = values).list.join("|"))
        )
    def get_df_from_key(to_parse: list, key: str, col_name: str = "Value", idx_name: str = "Subzone") -> pl.DataFrame:
        return (pl.DataFrame(
                data = [(i, to_parse[i][key]) for i in range(len(to_parse)) if ((key in to_parse[i]) and (to_parse[i][key] is not None))], 
                schema = [idx_name,col_name])
        )
    def get_df_from_list_key(to_parse: list, key: str, col_name: str = "Value", idx_name: str = "Subzone") -> pl.DataFrame:
        return (pl.DataFrame(
                data = [(i, "|".join(str(j) for j in to_parse[i][key])) for i in range(len(to_parse)) if ((key in to_parse[i]) and (to_parse[i][key] is not None))], 
                schema = [idx_name,col_name])
        )
    def get_df_from_key_nested(to_parse: list, key: str, label: str, variable: str, group_name: str = "Group", col_name: str = "Value", idx_name: str = "Subzone") -> pl.DataFrame:
        if label is None:
            return(pl.DataFrame(
                data = [(i, "All", to_parse[i][key][variable]) for i in range(len(to_parse)) if ((key in to_parse[i]) and (to_parse[i][key] is not None)) if variable in to_parse[i][key]], 
                schema = [idx_name, group_name, col_name])
            )
        else:
            return(pl.DataFrame(
                data = [(i, j[label], j[variable]) for i in range(len(to_parse)) if ((key in to_parse[i]) and (to_parse[i][key] is not None)) for j in to_parse[i][key] if (variable in j) and (label in j)], 
                schema = [idx_name, group_name, col_name])
            )
    def unknown_key_type_message(valtype, key):
        print(f'Unsupported {valtype} variable found in result, key name: {key}')
    def get_measure_and_summary_type(measure):  
        summary_type = "Unknown"
        summary_value_check = [x in measure for x in summary_metrics]
        if any(summary_value_check):
            summary_type = summary_metrics[summary_value_check.index(True)]
            measure = measure.replace(summary_type, "")
        return measure, summary_type
    
    query_info = []
    zone_info = []
    result_info = []
    for param in list(params.keys()):
        valtype = type(params[param])
        if valtype in [int, float, bool, str]:
            this_data = [params[param]]
        elif valtype is list:
            this_data = ["|".join(str(j) for j in params[param])]
        elif valtype is dict:
            this_data = [str(params[param])]
        else:
            print(f'Failed: {params[param]}')
            continue
        to_append = pl.DataFrame(
            data = this_data,
            schema = [param]
        )
        if to_append.height > 0: query_info.append(to_append)

    all_key_valtypes = set([(key, type(entry[key])) for entry in result for key in list(entry.keys())])
    for key_valtype in all_key_valtypes:
        key = key_valtype[0]
        valtype = key_valtype[1]
        if key in query_info_fields:
            if valtype in [int, float, bool, str]:
                to_append = get_df_from_key(result, key, col_name=key).drop("Subzone").drop_nulls(key).unique()
                if to_append.height > 0: query_info.append(to_append)
            if valtype in [list, dict]:
                to_append = get_df_from_list_key(result, key, col_name=key).drop("Subzone").drop_nulls(key).filter(pl.col(key).str.len_chars() > 0).unique()
                if to_append.height > 0: query_info.append(to_append)
        elif key in subzoneDescriptorsAll:
            to_append = get_df_from_key(result, key, col_name=key)
            if to_append.height > 0: zone_info.append(to_append)
        elif valtype in [int, float, bool]:
            measure, summary_type = get_measure_and_summary_type(key)
            this_result = get_df_from_key(result, key).with_columns(
                pl.lit("None").alias("Group_By"),
                pl.lit("All").alias("Group"),
                pl.lit(measure).alias("Measure"),
                pl.lit(summary_type).alias("Statistic"),
                pl.lit(np.nan).alias("Percentile")
            )
            if valtype is bool:
                this_result = this_result.with_columns(pl.col("Value").cast(pl.Float32))
            if this_result.height > 0: result_info.append(this_result)
        elif valtype is list:
            if "Percentile" in key:
                measure = key.replace("Percentiles", "").replace("Percentile", "")
                to_append = get_df_from_key_nested(result, key, label="Percentile", variable="Value", group_name="Percentile").with_columns(
                    pl.lit(measure).alias("Measure"),
                    pl.lit("Percentile").alias("Statistic"),
                    pl.lit("None").alias("Group_By"),
                    pl.lit("All").alias("Group"))
                if to_append.height > 0: result_info.append(to_append)
            elif ("Bins" in key) or ("By" in key):
                if "Bins" in key:
                    label = "Bin"
                    group_by_name = key.replace("Bins","Bin")
                else:
                    label = key.split("By",1)[1]
                    if label in group_name_labels:
                        label = group_name_labels[label]
                    group_by_name = label
                variables = set([(k,type(j[k])) for i in range(len(result)) if ((key in result[i]) and (result[i][key] is not None)) 
                                 for j in result[i][key] 
                                 for k in j.keys() if (
                                     (k != label) and 
                                     (k != group_by_name) and 
                                     ('Id' not in k) and 
                                     ('Index' not in k) and 
                                     ('Name' not in k)
                                     )])
                for variable in variables:
                    variable_name = variable[0]
                    variable_valtype = variable[1]
                    if variable_valtype is list:
                        sublabel = variable_name.split("By",1)[1]
                        if sublabel in group_name_labels:
                            sublabel = group_name_labels[sublabel]
                        subgroup_by_name = sublabel
                        subvariables = set([(l,type(k[l])) 
                            for i in range(len(result)) if ((key in result[i]) and (result[i][key] is not None)) 
                            for j in result[i][key] if ((variable in result[i][key]) and (result[i][key][variable] is not None)) 
                            for k in j[variable] if type(k) is dict
                            for l in k.keys() if (
                                (l != sublabel) and 
                                (l != subgroup_by_name) and 
                                ('Id' not in l) and 
                                ('Index' not in l) and 
                                ('Name' not in l)
                                )])
                        for subvariable in subvariables:
                            subvariable_name = subvariable[0]
                            measure, summary_type = get_measure_and_summary_type(subvariable_name)
                            to_append = (
                                pl.DataFrame(
                                    data = [(i, f'{j[label]}_{k[sublabel]}', k[subvariable_name]) 
                                            for i in range(len(result)) if ((key in result[i]) and (result[i][key] is not None)) 
                                            for j in result[i][key] if ((variable_name in result[i][key]) and (result[i][key][variable_name] is not None)) 
                                            for k in j[variable_name] if (type(k) is dict and subvariable_name in k)],
                                    schema = ["Subzone", "Group", "Value"])
                                .with_columns(
                                    pl.lit(measure).alias("Measure"),
                                    pl.lit(summary_type).alias("Statistic"),
                                    pl.lit(f'{group_by_name}_{subgroup_by_name}').alias("Group_By"),
                                    pl.lit(np.nan).alias("Percentile"))
                            )
                            if to_append.height > 0: result_info.append(to_append)
                    else:
                        measure, summary_type = get_measure_and_summary_type(variable_name)
                        to_append = get_df_from_key_nested(result, key, label=label, variable=variable_name).with_columns(
                            pl.lit(measure).alias("Measure"),
                            pl.lit(summary_type).alias("Statistic"),
                            pl.lit(group_by_name).alias("Group_By"),
                            pl.lit(np.nan).alias("Percentile"))
                        if to_append.height > 0: result_info.append(to_append)
            else:
                unknown_key_type_message(valtype, key)
        elif valtype is dict:
            variables = set([j for i in range(len(result)) if ((key in result[i]) and (result[i][key] is not None)) for j in result[i][key]
                             if (('Id' not in j) and ('Index' not in j) and ('ID' not in j) and ('Idx' not in j))])
            for variable in variables:
                measure, summary_type = get_measure_and_summary_type(variable)
                to_append = get_df_from_key_nested(result, key, label=None, variable=variable).with_columns(
                    pl.lit(measure).alias("Measure"),
                    pl.lit(summary_type).alias("Statistic"),
                    pl.lit("None").alias("Group_By"),
                    pl.lit(np.nan).alias("Percentile"))
                if to_append.height > 0: result_info.append(to_append)
        elif valtype is type(None):
            continue
        else:
            unknown_key_type_message(valtype, key)

    query_info_df = pl.concat(query_info, how="horizontal").unique()
    print(query_info_df)
    query_id_names_all_found = [value for value in ["id","JobKey","Query_ID","QueryID"] if value in query_info_df.columns]
    query_info_df = (query_info_df.select(pl.coalesce(query_id_names_all_found).drop_nulls().first().alias("Query_ID"))
        .with_columns(
            pl.lit(user_params.accounts[user_params.db]['db']).alias("DB"),
            pl.lit(user_params.accounts[user_params.db]['email']).alias("User_ID"),
            pl.lit(datetime.now(timezone.utc)).alias("Query_Processed_Time"),
            pl.lit(params['queryType']).alias("Query_Type")
        )
    )
    for df in query_info:
        query_info_df = query_info_df.join(df, how="cross")

    this_query_id = query_info_df.select(pl.col("Query_ID").first()).item()
    zone_info_df = (pl.concat(zone_info, how="diagonal_relaxed")
        .select("Subzone")
        .unique()
        .with_columns(pl.lit(this_query_id).alias("Query_ID"))
    )
    for df in zone_info:
        zone_info_df = zone_info_df.join(df, how="left", on="Subzone")

    result_info_df = (pl.concat(result_info, how="diagonal_relaxed")
        .unique()
        .with_columns(
            pl.lit(this_query_id).alias("Query_ID"),
            pl.col("Value").cast(pl.Float64))
        .select("Query_ID", "Subzone", "Group_By", "Group", "Measure", "Statistic", "Percentile","Value")
    )
    if (len(result_info_df) == 0) | (result_info_df.height==0):
        print("Result had no content (result_info_df.height==0).")

    combined = QueryResult(params, query_info_df, subzone_info = zone_info_df, results = result_info_df, json = result)
    if serialize:
        if out_path is not None:
            combined.query_info.write_parquet(out_path + f'{this_query_id}_metadata.parquet')
            combined.subzone_info.write_parquet(out_path + f'{this_query_id}_subzones.parquet')
            combined.results.write_parquet(out_path + f'{this_query_id}_results.parquet')
        else:
            print("Cannot serialize parsed results because out_path was not specified!")

    return combined

def create_zone_from_counties(client, county_list, name, comments=""):
    if type(county_list[0]) is dict:
        county_list = [county['ZoneId'] for county in county_list]

    get_params = {
        'queryType': 'getZones',
        'zoneType': 'County',
        'filters': [{'RegionIds': county_list, 'RegionType': 'County'}]
    }
    job_ids, job_statuses, job_outputs1 = query_api(client, get_params, out_path = None, process = False)
    result_json = job_outputs1[0]

    geographies = [fips['Geography'] for fips in result_json[1:(len(result_json))]]
    loaded = [shape(json.loads(geography)).buffer(0) for geography in geographies]
    all_fips_union = dumps(gpd.GeoSeries(loaded).unary_union )#.convex_hull)

    if comments == "":
        comments = "Combines county FIPS: "
        for idx in range(len(county_list)):
            comments += county_list[idx]
            if idx != len(county_list) - 1:
                comments += ", "
            else:
                comments += "."

    create_params = {
        "queryType": "createZone",
        "zone": {
            "Comments": comments,
            "CustomerZoneId": name,
            "ZoneGeography": all_fips_union,
            "ZoneName": name,
            "ZoneTypeIds": f'[{altitude_params.region_zone_type_id[altitude_params.db]}]',
        }
    }
    job_ids, job_statuses, job_outputs2 = query_api(client, create_params, out_path = None, process = False)
    result_json2 = job_outputs2[0]
    return result_json2[0]['id']

def create_geojson_from_counties(client, county_list, out_path):
    if type(county_list[0]) is dict:
        county_list = [county['ZoneId'] for county in county_list]

    get_params = {
        'queryType': 'getZones',
        'zoneType': 'County',
        'filters': [{'RegionIds': county_list, 'RegionType': 'County'}]
    }
    job_ids, job_statuses, job_outputs = query_api(client, get_params, out_path, process = False)
    result_json = job_outputs[0]

    geographies = [fips['Geography'] for fips in result_json[1:(len(result_json))]]
    loaded = [shape(json.loads(geography)).buffer(0) for geography in geographies]
    all_fips_union = dumps(gpd.GeoSeries(loaded).unary_union )#.convex_hull)
    return all_fips_union, loaded, result_json

def parse_serialized_results(out_path):
    params = get_params_in_folder(out_path, drop_id = False) #list[dict]
    results = get_results_in_folder(out_path) #list[dict]
    assert len(results) == len(params)
    for idx in range(len(results)):
        parse_query_output(results[idx], params[idx], out_path = out_path, serialize = True)

    
def combine_and_clean_parquets(out_path): 
    metadata = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*metadata.parquet")]
    pl.concat(metadata, how="diagonal_relaxed").unique().collect().write_parquet(out_path + "all_metadata_new.parquet")
    subzones = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*subzones.parquet")]
    pl.concat(subzones, how="diagonal_relaxed").unique().collect().write_parquet(out_path + "all_subzones_new.parquet")
    results = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*results*parquet")]
    pl.concat(results, how="diagonal_relaxed").unique().collect().write_parquet(out_path + "all_results_new.parquet")

def combine_and_clean_parquets_old(out_path):
    metadata = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*metadata.parquet")]
    pl.concat(metadata, how="diagonal_relaxed").collect().unique().write_parquet(out_path + "all_metadata_new.parquet")

    subzones1 = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*subzone_definitions.parquet")]
    if len(subzones1) > 0:
        subzones1 = pl.concat(subzones1, how="diagonal").unique()
    else:
        subzones1 = pl.LazyFrame()
    subzones2 = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "*subzones.parquet")]
    if len(subzones2) > 0:
        subzones2 = pl.concat(subzones2, how="diagonal").unique()
    else:
        subzones2 = pl.LazyFrame()

    subzones_all = (pl.concat([subzones1, subzones2,], how="diagonal_relaxed").collect()
        #.pipe(fix_alternate_labels)
        ).unique()
    
    subzoneDescriptors = list(set(subzoneDescriptorsAll) & set(subzones_all.columns))  
    (subzones_all
        .select(subzoneDescriptors)
        .unique()
    ).write_parquet(out_path + "all_subzone_definitions_new.parquet")

    shapes_old = [
        pl.scan_parquet(file)
        for file in glob.glob(out_path + "all_subzone_shapes.parquet")]
    if len(shapes_old) > 0:
        shapes_old = pl.concat(shapes_old, how="diagonal").unique().collect()
    else:
        shapes_old = pl.DataFrame()

    if "RegionId" in subzones_all.columns:
        shapes_new = subzones_all.select(["ZoneId","Geography"]).filter(pl.col("Geography").is_not_null()).unique()
        shapes_new = pl.concat([shapes_old, shapes_new], how="diagonal_relaxed").unique().group_by("ZoneId").agg(pl.col("Geography").first())
        shapes_new.write_parquet(out_path + "all_subzone_shapes_new.parquet")

    pl.read_parquet(out_path + "*results*parquet").unique().write_parquet(out_path + "all_results_new.parquet")