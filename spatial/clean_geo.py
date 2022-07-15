from urllib.request import urlopen
import requests, io
import json
import numpy as np

def GeoOpen(filename, geo):
    '''A function to read in spatial data'''
    if geo == 'yes':
        file_type = '.geojson'
    else:
        file_type = '.json'
    file_path = 'https://raw.githubusercontent.com/alisahartwell/data_general/main/' + filename + file_type
    response = urlopen(file_path)
    return json.loads(response.read())


def PrecisionSpec(collection, level):
    '''Takes in FeatureCollection, outputs lat-long coords at specified precision'''

    for feature in range(len(collection['features'])):
        if collection['features'][feature]['geometry']['type'] == 'MultiPolygon':
            for sub_feature in range(len(collection['features'][feature]['geometry']['coordinates'])):
                for polygon in range(len(collection['features'][feature]['geometry']['coordinates'][sub_feature])):
                    for point in range(len(collection['features'][feature]['geometry']['coordinates'][sub_feature][polygon])):                
                        pair = collection['features'][feature]['geometry']['coordinates'][sub_feature][polygon][point]                        
                        collection['features'][feature]['geometry']['coordinates'][sub_feature][polygon][point] = [np.round(i,level) for i in pair]        
        if collection['features'][feature]['geometry']['type'] == 'Polygon':
            for sub_feature in range(len(collection['features'][feature]['geometry']['coordinates'])):
                for point in range(len(collection['features'][feature]['geometry']['coordinates'][sub_feature])):
                    pair = collection['features'][feature]['geometry']['coordinates'][sub_feature][point]
                    collection['features'][feature]['geometry']['coordinates'][sub_feature][point] = [np.round(i,level) for i in pair]
    return collection


def BoroughFilter(collection, boro_field, boro_name):
    '''Takes in feature collection, filters by boro name'''
    selected_features = []
    for feature in range(len(collection['features'])): 
        if collection['features'][feature]['properties'][boro_field] in boro_name:
            selected_features.append(collection['features'][feature])
            
    fresh_feature = {'type': 'FeatureCollection',
                    'features': selected_features}
    return fresh_feature

