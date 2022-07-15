import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import re

def CleanEnroll(df, geo_status, eligible, min_date=False, max_date=False):
    '''Reads in Enrollments table from SQL Server. Creates datetime index, optinally filters by dates, 
    cleans override columns, optionally cleans address column'''
    df['StartDate'] = df['StartDate'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))
    if min_date and max_date:
        min_date = dt.datetime.strptime(min_date, '%m/%d/%Y')
        max_date = dt.datetime.strptime(max_date, '%m/%d/%Y')
        df = df[(df['StartDate'] >= min_date) &
                                 (df['StartDate'] <= max_date)]


    # cleaning up override columns
    df.loc[:, 'ServiceLevel'] = np.where(df['ServiceLevel'].isin(['Override Advice to Intensive',
                                                                           'Override Full to Intensive']),
                                           'Intensive', 
                                           df['ServiceLevel'])
    df.loc[:, 'ServiceLevel'] = np.where(df['ServiceLevel'].isin(['Override Advice to Full',
                                                                            'Override Intensive to Full']),
                                           'Full',
                                           df['ServiceLevel'])

    df.loc[:, 'ServiceLevel'] = np.where(df['ServiceLevel'].isin(['Override Intensive to Advice',
                                                                            'Override Full to Advice']),
                                           'Advice',
                                           df['ServiceLevel'])
    if geo_status is True:
        df['Zip'] = df['Zip'].astype('str')
        df['Zip'] = df['Zip'].str.split('.', expand=True)[0]
    if eligible is True:
        df = df[df['Determination'] == 'HB Eligible']
    return df.drop_duplicates()
    
def EnrollGroup(df, column):
    
    headers = {
    'selector': 'th:not(.index_name)',
    'props': 'background-color: #8a004c; color: white;'
    }
    df['LastCase'] = df.groupby(['FirstName', 'LastName', 'Street'])['StartDate'].transform(lambda x: max(x))
    df = df[df['StartDate'] == df['LastCase']]
    df = df.sort_values('RAQ', ascending=False).drop_duplicates(subset=['FirstName', 'LastName', 'Street', 'StartDate'])
    x = df.groupby(column).agg(num_cases=('CaseNumber', 'count')).reset_index()
    enroll_cases = df['CaseNumber'].nunique()
    x['% of Total'] = round(100 * x['num_cases']/enroll_cases, 0)
    x = x.sort_values('num_cases', ascending=False)
    x['% of Total'] = x['% of Total'].astype(int)
    x = x.rename(columns = {'num_cases': 'Number of Cases'})
    x = x.head(25)
    
    y = x.style.set_properties(**{'font-weight': 'bold',
                                 'background-color' : '#faedaf', 'text-align': 'center'}, subset=column)
    y.set_table_styles([headers]).hide_index()
    return y

def SQLPrep(file_name, header=2):
    '''Reads in excel file of enrollments and prepares it for SQL Server'''
    enrollments = pd.read_excel(file_name, header=header)
    enrollments.columns = [i.replace(' ', '') for i in enrollments.columns]
    enrollments.columns = [i.replace('#of', '') for i in enrollments.columns]
    enrollments = enrollments.rename(columns={'FIrstName' : 'FirstName',
                                             'Apt/Unit' : 'Apt_Unit'})
    enrollments = enrollments.drop_duplicates(subset=['CaseNumber', 'LastName', 'FirstName'],keep='last')


    enrollments['StartDate'] = enrollments['StartDate'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y').date())
    enrollments['Zip'] = enrollments['Zip'].astype('str')
    enrollments['Zip'] = enrollments['Zip'].str.split('.', expand=True)[0]
    enrollments['CD'] = enrollments['CD'].fillna(0.0).astype('int')
    enrollments = enrollments.set_index('CaseNumber')
    enrollments = enrollments.where(pd.notnull(enrollments), None)
    return enrollments


def UpdateEnroll(new_file, default_path=True, header=2):
    '''Takes in Enrollments report from iCares Reporting and updates Enrollment table on the SQL Server.
    As Enrollments may get new data over time such as CM or become finalized, any Case Numbers in the new
    file are dropped from the database prior to insertion. Defaults to file path in Data_General/Enrollments'''
    if default_path is True:
        file_name = 'c:/users/alisah/Documents/Data_General/Enrollment_Files/' + new_file
    else:
        file_name = new_file
    new_enroll = SQLPrep(file_name, header)

    def PrintDim(df):
        '''Easy way to print table dimensions during upsert'''
        return print(f'The Enrollment table is {df.shape[0]} rows and {df.shape[1]} columns.')

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=S21PVM02.MO.CAMBA.ORG,1433;'
                          'Database=HomeBase;'
                          'Trusted_Connection=yes'
                         )

    enroll_db = pd.read_sql_query('SELECT * FROM Enrollments', conn)
    conn.close()
    PrintDim(enroll_db)
    enroll_db = enroll_db.set_index('CaseNumber')
    

    # Cases to delete from DB
    to_remove = tuple(enroll_db.index[enroll_db.index.isin(new_enroll.index)])
    to_remove_str = str(to_remove)
    new_enroll_sql = new_enroll.reset_index().values.tolist()
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=S21PVM02.MO.CAMBA.ORG,1433;'
                          'Database=HomeBase;'
                          'Trusted_Connection=yes'
                         )
    drop_duplicate_cases = '''DELETE FROM Enrollments WHERE CaseNumber IN ''' + to_remove_str
    cursor = conn.cursor()
    cursor.execute(drop_duplicate_cases)
    print(f'Dropping {len(to_remove)} rows from the Enrollments table.')
    insert_to_tbl_stmt = "INSERT INTO Enrollments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    cursor.fast_executemany = True
    cursor.executemany(insert_to_tbl_stmt, new_enroll_sql)
    print(f'{len(new_enroll)} rows inserted to the Enrollments table')
    enroll_test = pd.read_sql_query('SELECT * FROM Enrollments', conn)
    PrintDim(enroll_test)
    cursor.commit()
    conn.commit()
    conn.close()