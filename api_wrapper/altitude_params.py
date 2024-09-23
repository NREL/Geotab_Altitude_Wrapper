from . import user_params

region_zone_type_id = {'example_db': 'zt-tlpg24qkjs'}
richmond_grouped_zone_id = 'z-acif6vi4nxc'
richmond_zones = [{"ZoneId":"51041","ISO_3166_2":"US-VA","ZoneType":"County"},
                             {"ZoneId":"51085","ISO_3166_2":"US-VA","ZoneType":"County"},
                             {"ZoneId":"51087","ISO_3166_2":"US-VA","ZoneType":"County"},
                             {"ZoneId":"51760","ISO_3166_2":"US-VA","ZoneType":"County"}]

september21 = {"DateFrom": "2021-09-08", "DateTo": "2021-09-30"}
february22 = {"DateFrom": "2022-02-01", "DateTo": "2022-02-28"}

timeRangesAllStopAnalytics = [{'TimeFrom': '00:00:00.000', 'TimeTo': '23:59:59.000'},
              {'TimeFrom': '00:00:00.000', 'TimeTo': '00:59:59.000'},
              {'TimeFrom': '01:00:00.000', 'TimeTo': '01:59:59.000'},
              {'TimeFrom': '02:00:00.000', 'TimeTo': '02:59:59.000'},
              {'TimeFrom': '03:00:00.000', 'TimeTo': '03:59:59.000'},
              {'TimeFrom': '04:00:00.000', 'TimeTo': '04:59:59.000'},
              {'TimeFrom': '05:00:00.000', 'TimeTo': '05:59:59.000'},
              {'TimeFrom': '06:00:00.000', 'TimeTo': '06:59:59.000'},
              {'TimeFrom': '07:00:00.000', 'TimeTo': '07:59:59.000'},
              {'TimeFrom': '08:00:00.000', 'TimeTo': '08:59:59.000'},
              {'TimeFrom': '09:00:00.000', 'TimeTo': '09:59:59.000'},
              {'TimeFrom': '10:00:00.000', 'TimeTo': '10:59:59.000'},
              {'TimeFrom': '11:00:00.000', 'TimeTo': '11:59:59.000'},
              {'TimeFrom': '12:00:00.000', 'TimeTo': '12:59:59.000'},
              {'TimeFrom': '13:00:00.000', 'TimeTo': '13:59:59.000'},
              {'TimeFrom': '14:00:00.000', 'TimeTo': '14:59:59.000'},
              {'TimeFrom': '15:00:00.000', 'TimeTo': '15:59:59.000'},
              {'TimeFrom': '16:00:00.000', 'TimeTo': '16:59:59.000'},
              {'TimeFrom': '17:00:00.000', 'TimeTo': '17:59:59.000'},
              {'TimeFrom': '18:00:00.000', 'TimeTo': '18:59:59.000'},
              {'TimeFrom': '19:00:00.000', 'TimeTo': '19:59:59.000'},
              {'TimeFrom': '20:00:00.000', 'TimeTo': '20:59:59.000'},
              {'TimeFrom': '21:00:00.000', 'TimeTo': '21:59:59.000'},
              {'TimeFrom': '22:00:00.000', 'TimeTo': '22:59:59.000'},
              {'TimeFrom': '23:00:00.000', 'TimeTo': '23:59:59.000'}]

timeRangesAllODAnalytics = [dict(item, **{'Component':'Start'}) for item in timeRangesAllStopAnalytics]

industriesAll = [
    48,
    23,
    53,
    54,
    21,
    92,
    42,
    81,
    56,
    33,
    None,
    44,
    62,
    32,
    22,
    71,
    55,
    52,
    11,
    31,
    49,
    51,
    45,
	61,
	91,
	41]

percentilesAll = [1, 5, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95, 99]
singleDaysOfWeek = [[1],[2],[3],[4],[5],[6],[7]]
business_days = [2,3,4,5,6]

months = [september21, february22]
db_zones = [
    {'ZoneId': 'z-acif6vi4nxc', 'ISO_3166_2': None, 'ZoneType': 'Custom'}, 
    {'ZoneId': 'z-hgr7g97narp', 'ISO_3166_2': None, 'ZoneType': 'Custom'},
    {'ZoneId': 'z-vx03tw3zc89', 'ISO_3166_2': None, 'ZoneType': 'Custom'}]
classes_and_schemes = [{"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class G"}],
    "vehicleClassSchemeId":4},
    {"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class H"}],
    "vehicleClassSchemeId":4},
    {"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class 3"}],
    "vehicleClassSchemeId":3},
    {"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class 4"}],
    "vehicleClassSchemeId":3},
    {"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class 5"}],
    "vehicleClassSchemeId":3},
    {"vehicleClasses":[{"VehicleType":"Truck","WeightClass":"Class 6"}],
    "vehicleClassSchemeId":3},
    {"vehicleClasses":[{"VehicleType":"MPV","WeightClass":"*"}],
    "vehicleClassSchemeId":3},
    {"vehicleClasses":[{"VehicleType":"#Other","WeightClass":"*"}],
    "vehicleClassSchemeId":3}]    
veh_classes = [
    {'VehicleType': 'Truck', 'WeightClass': 'Class G'},
    {'VehicleType': 'Truck', 'WeightClass': 'Class H'},
    {'VehicleType': 'Truck', 'WeightClass': 'Class 3'},
    {'VehicleType': 'Truck', 'WeightClass': 'Class 4'},
    {'VehicleType': 'Truck', 'WeightClass': 'Class 5'},
    {'VehicleType': 'Truck', 'WeightClass': 'Class 6'},
    {'VehicleType': 'MPV', 'WeightClass': '*'},
    {'VehicleType': '#Other', 'WeightClass': '*'}]
daysOfWeekAll = [[1,2,3,4,5,6,7]] + singleDaysOfWeek
vocations = [1,2,3]

defaultODParams = {
    'queryType': 'getOriginDestinationMatrix',
    'connectorAndCondition': False,
    'zoneOrigins': [],
    'zoneConnectors': [],
    'zoneNotConnectors': [],
    'zoneDestinations': [],
    'componentTimeRange': {'TimeFrom': '00:00:00.000', 'TimeTo': '23:59:59.000', 'Component': 'Start'},
    'isMetric': False,
    'trendAnalysis': False,
    'percentiles': [1, 5, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95, 99],
    'retainBackwardChainingOrigin': True,
    'retainForwardChainingDestination': True,
    'tripChainDuration': 5,
    'vehicleClassSchemeId': 3,
    'dateRanges': months,
    'daysOfWeek': business_days,
    'vehicleClasses': [entry['vehicleClasses'][0] for entry in classes_and_schemes]
}

defaultRegionalDomicileParams = {
'queryType': 'getRegionalDomicileAnalytics',
'isMetric': False,
'zones': db_zones,
'percentiles': percentilesAll,
'NAICS': [],
'vocations': vocations,
'vehicleClassSchemeId': 3,
'vehicleClasses': veh_classes,
}

defaultStopAnalyticsParams = {
'queryType': 'getStopAnalytics',
'isMetric': False,
'zones': db_zones,
'aggregationMode': 2,
'bucketHours': True,
'dateRanges': months,
'daysOfWeek': [],
'percentiles': percentilesAll,
'percentileThreshold': 0,
'stopDurationBins': [60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900, 960, 1020, 1080, 1140, 1200, 1260, 1320, 1380, 1440], 
'NAICS': [],
'vocations': vocations,
'vehicleClassSchemeId': 3,
'vehicleClasses': veh_classes,
'includeGeography': True
}
