import re
import http.client, urllib.request, urllib.parse, urllib.error, base64, json
import pandas as pd
import pyodbc
import sys
sys.path.insert(0, 'c:/users/alisah/documents/Functions/')
from enrollment_functions import enroll
import difflib
import numpy as np


def AddressNorm(df, street_col="Street", boro='City', zipcode='Zip'):
    '''A function to normalize addresses. Reads in data frame with separate columns for street,
    city, and zip. Standardizes street address, then creates address_clean column in df.
    Default values set for Enrollment Report.'''
    df[street_col] = df[street_col].fillna('')
    df[street_col] = df[street_col].str.upper()
    df[boro] = df[boro].str.upper()
    # For 3RD street_col and the like, remove "RD"
    df[street_col] = df[street_col].str.replace("(\\bAPT).+$|\\bAPT$", "", regex=True)
    df[street_col] = df[street_col].str.replace("(\\bUNIT).+$|\\bUNIT$", "", regex=True)
    df[street_col] = df[street_col].str.replace(",", "", regex=True)
    df[street_col] = df[street_col].str.replace("  ", " ", regex=True)
    df[street_col] = df[street_col].str.replace(".", "", regex=True)
    df['num_name'] = df[street_col].str.extract('\\b([0-9]+)([A-z][A-z])\\b')[1]
    df['num_name'] = df['num_name'].fillna('')
    df[street_col] = [a.replace(b, '').strip() for a, b in zip(df[street_col], df['num_name'])]
    # For addresses where the house number has a letter in it
    df['letter_num'] = df[street_col].str.extract('\\b([0-9]+)([A-z])\\b')[1]
    df['letter_num'] = df['letter_num'].fillna('')
    df[street_col] = [a.replace(b, '').strip() for a, b in zip(df[street_col], df['letter_num'])]
    df = df.drop(columns={'num_name', 'letter_num'})
    # Standardize street_col types
    street = ("(\\bST$|\\bST[.]$|\\bST\\s$|\\bST[.]\\s$|\\bSTR$)", "STREET")
    avenue = ("(\\bAVE$|\\bAVE[.]$|\\bAVE\\s$|\\bAVE[.]\\s$|\\bAVE\\b | \\bAVE[[:punct:]])", "AVENUE")
    road = ("(\\bRD$|\\bRD[.]$|\\bRD\\s$|\\bRD[.]\\s$)", "ROAD") 
    lane = ("(\\bLN$|\\bLN[.]$|\\bLN\\s$|\\bLN[.]\\s$)", "LANE")
    place = ("(\\bPL$|\\bPL[.]$|\\bPL\\s$|\\bPL[.]\\s$)", "PLACE")
    boulevard = ("(\\bBLVD$|\\bBLVD[.]$|\\bBLVD\\s$|\\bBLVD[.]\\s$)", "BOULEVARD")
    drive = ("(\\bDR$|\\bDR[.]$|\\bDR\\s$|\\bDR[.]\\s$)", "DRIVE")   
    court = ("(\\bCT$|\\bCT[.]$|\\bCT\\s$|\\bCT[.]\\s$)", "COURT")  
    parkway = ("(\\bPKWY$|\\bPKWY[.]$|\\bPKWY\\s$|\\bPKWY[.]\\s$)", "PARKWAY")
    circle = ("(\\bCIR$|\\bCIR[.]$|\\bCIR\\s$|\\bCIR[.]\\s$)", "CIRCLE")
    # directional names
    west = ("(\\s\\bW\\s|\\s\\bW[.]\\s|\\s\\bW$|\\s\\bW[.]$)", " WEST ")
    south = ("(\\s\\bS\\s|\\s\\bS[.]\\s|\\s\\bS$|\\s\\bS[.]$)", " SOUTH ")
    north = ("(\\s\\bN\\s|\\s\\bN[.]\\s|\\s\\bN$|\\s\\bN[.]$)", " NORTH ")
    east = ("(\\s\\bE\\s|\\s\\bE[.]\\s|\\s\\bE$|\\s\\bE[.]$)", " EAST ")
    saint = ("(\\s\\bSAINT\\s|\\s\\bST[.]\\s)"," SAINT ")
    possible_streets = [street, avenue, road, lane, place, boulevard,
                        drive, circle, court, parkway,
                        north, south, east, west, saint]
    for street_tuple in possible_streets:
        regex, correct_name = street_tuple
        wrong_name = re.compile(regex)
        df[street_col] = df[street_col].str.replace(wrong_name, correct_name)        

   # Replace number words     
    number_words = ["FIRST" , "SECOND" , "THIRD" , "FOURTH" , "FIFTH" ,
                    "SIXTH" , "SEVENTH" , "EIGHTH" , "NINTH" , "TENTH"]
    numbers = ["1" , "2" , "3" , "4" , "5" ,
               "6" , "7" , "8" , "9" , "10"]  
    name_to_num = {number_words[i]: numbers[i] for i in range(len(number_words))} 
    df[street_col] = df[street_col].replace(name_to_num, regex=True)
    df[street_col] = df[street_col].str.replace('SAINT', 'ST')
    df[street_col] = df[street_col].str.replace('\\s+', ' ', regex=True)
    df[street_col] = df[street_col].str.replace('\\s$', '', regex=True)
    df[street_col] = df[street_col].str.replace("'", '')
    df[street_col] = df[street_col].str.strip()
    df['address_clean'] = df[[street_col, boro, zipcode]].fillna('').apply(lambda x: ' '.join(x), axis=1)
    df['address_clean'] = df['address_clean'].str.replace('  ', ' ',regex=True)
    return df

def AddressCheck(table):
    'Normalizes and pulls unique addresses from the specified table; currently only works for Enrollments'
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=S21PVM02.MO.CAMBA.ORG,1433;'
                          'Database=HomeBase;'
                          'Trusted_Connection=yes'
                         )
    query_stmt = 'SELECT * FROM ' + table
    db_table = pd.read_sql_query(query_stmt, conn)
    conn.close()
    if table == 'Enrollments':
        final_table = enroll.CleanEnroll(db_table, True, True)

    geo_new = AddressNorm(final_table, 'Street')
    new = set(geo_new['address_clean'])
    return new


def GetGeocoding(address):
    '''Takes an address string or strings and outputs a dataframe with full geocoding'''
    sub_key = '0b84401dce4f4cab8e92be367f59aedd'
    headers = {
    # Request headers
    'Ocp-Apim-Subscription-Key': sub_key,
    }
    params = urllib.parse.urlencode({'input': address
    })
    try:
        conn = http.client.HTTPSConnection('api.nyc.gov')
        conn.request("GET", "/geo/geoclient/v1/search.json*?%s" % params, "{body}", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))   
    x = data.decode('utf-8')
    geocoded = json.loads(x)
    if x:
        address_data = pd.json_normalize(geocoded,
                         record_path = ['results'],
                         meta = ['input'])
        if address_data.shape[0] != 0:
            address_final = address_data.copy()
            address_final = address_final.rename(columns = {'response.bbl':'bbl',
                                                          'response.buildingIdentificationNumber' : 'bin',
                                                          'response.bblBoroughCode' : 'boro_code',
                                                          'response.censusTract2010':'tract_2010',
                                                          'response.houseNumber' : 'house_num',
                                                          'response.streetName1In': 'street_name',
                                                          'response.uspsPreferredCityName' : 'boro',
                                                          'response.zipCode':'zip',
                                                          'response.latitudeInternalLabel':'lat', 
                                                          'response.longitudeInternalLabel':'lng',
                                                          'response.giHighHouseNumber1':'house_num_high', 
                                                          'response.giLowHouseNumber1': 'house_num_low',
                                                          'response.bblTaxBlock' : 'block',
                                                          'response.bblTaxLot' : 'lot', 
                                                          'response.censusBlock2010' : 'block_2010', 
                                                         'response.cityCouncilDistrict' : 'ccd',
                                                         'response.communityDistrict' : 'cd',
                                                          'response.congressionalDistrict' : 'cong_dist',
                                                          'response.assemblyDistrict' : 'ad',
                                                          'response.stateSenatorialDistrict' : 'ssd',
                                                          'response.ntaName' : 'nta',
                                                          'response.condominiumBillingBbl' : 'condo_bbl',
                                                          'response.policePrecinct' : 'pp'
                                                         })

            address_final['no_results'] = False      
            return address_final

def GeocodeData(address_set):
    geocoded_locations = []
    for address in address_set:
        location = GetGeocoding(address)
        if location is not None:
            loc_cols = location.columns
            geo_cols = ['house_num', 'street_name', 'boro',
                                 'zip', 'bbl', 'no_results', 'bin',
                                 'boro_code', 'block', 'lot', 'block_2010',
                                 'tract_2010', 'cd', 'ad', 'ccd', 'cong_dist',
                                 'pp', 'ssd', 'nta', 'lat', 'lng', 'condo_bbl', 'input']
            for i in geo_cols:
                if i not in loc_cols:
                    location[i] = 'NA'
            location = location[geo_cols]
            geocoded_locations.append(location)
    geocoded_final = pd.concat(geocoded_locations)
    return geocoded_final
                

def ExistingAddress():
    'Compiles a set of all existing addresses within the geoInfo table'
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=S21PVM02.MO.CAMBA.ORG,1433;'
                          'Database=HomeBase;'
                          'Trusted_Connection=yes'
                         )
    geo_old = pd.read_sql_query('SELECT * FROM geoInfo', conn)

    conn.close()
    geo_old['address'] = geo_old[['house_num', 'street_name',
                       'boro','zip']].fillna('').apply(lambda x: ' '.join(x), axis=1)
    old = set(geo_old['address'])
    return old

def MultipleMatches(address_set):
    # If multiple address matches returned, selects the best one. Input results of 
    # GeocodeData() and outputs data frame with one row per original address.
    # NOTE: tosses results where best match is not sufficiently similar to the original
    address_set['address'] = address_set[['house_num', 
                                              'street_name',
                                              'boro',
                                              'zip']].fillna('').apply(lambda x: ' '.join(x), axis=1)

    possibilities = address_set.groupby('address').filter(lambda x: len(x) > 1)
    address_set = address_set.groupby('address').filter(lambda x: len(x) == 1)
    maybe_address = set(possibilities['address'])
    possibilities['input'] = possibilities['input'].str.upper()

    possibilities['close_key'] = possibilities['input'].apply(lambda x: difflib.get_close_matches(word=x.rstrip(), 
                                                                                                possibilities=maybe_address,
                                                                                                n=1,
                                                                                               cutoff=0.8))

    possibilities = possibilities.explode('close_key')
    possibilities = possibilities[possibilities['address'] == possibilities['close_key']]
    possibilities = possibilities.drop_duplicates()

    address_set = pd.concat([address_set, possibilities])
    address_set = address_set.drop(columns= ['close_key', 'address', 'input'])
    return address_set

def MakeFips(df, geo_type, col_name):
    geocoded_final = df.copy()
    geocoded_final['boro'] = geocoded_final['boro'].str.upper()
    boros = [geocoded_final['boro'] == 'BROOKLYN',
             geocoded_final['boro'] == 'STATEN ISLAND']
    boro_num = ['3', '5']
    geocoded_final['boro_code'] = np.select(boros, boro_num)
#     geocoded_final['tract_scratch'] = geocoded_final['tract_2010'].str.split(".").str[0]
    geocoded_final['tract_scratch'] = geocoded_final['tract_2010'].str.replace(' ', '0')
    geocoded_final['tract_scratch'] = np.where(geocoded_final['tract_scratch'].str.len() <= 3,
                                        geocoded_final['tract_scratch'] + '00',
                                        geocoded_final['tract_scratch'])

    geocoded_final['tract_scratch'] = geocoded_final['tract_scratch'].str.pad(6, side='left', fillchar='0')
    if geo_type == 'tracts':
        geocoded_final[col_name] = geocoded_final['boro_code'] + geocoded_final['tract_scratch'] 
    else:
        geocoded_final[col_name] = geocoded_final['boro_code'] + geocoded_final['tract_scratch'] + geocoded_final['block_2010']
    return geocoded_final
