import pandas as pd
import os

### Reads in the file given the year, district name, and subdistrict name 

def read_file(year, state_name, district_name):
    
    return (pd.read_html('/Users/stanleyou/india_health_data/data/' + str(year) + '/MonthUpToMarch/' + state_name + '/' +    district_name + '/All.xls')[0])

### Cleans the necessary tables 
def clean_table(df, state_name, district_name, year):
    df.columns = df.columns.get_level_values(1)

    df = (df
 .rename(columns={'Number of Pregnant women registered within first trimester':'Subdistrict Name'})
 .set_index('Subdistrict Name')
 .iloc[1:50, :]
 #.insert(0, 'Year', [year] * len(df))          
)
    df.insert(0, 'Year', [year] * (len(df)))
    df.insert(0, 'District Name', [district_name] * (len(df)))
    df.insert(0, 'State Name', [state_name] * (len(df)))
    return df


def append_sub_district(district, sub_district):
    year_2012 = read_file(2012, district, sub_district)
    cleaned_2012 = clean_table(year_2012, district, sub_district, 2012)
    year_2013 = read_file(2013, district, sub_district)
    cleaned_2013 = clean_table(year_2013, district, sub_district, 2013)
    year_2014 = read_file(2014, district, sub_district)
    cleaned_2014 = clean_table(year_2014, district, sub_district, 2014)
    year_2015 = read_file(2015, district, sub_district)
    cleaned_2015 = clean_table(year_2015, district, sub_district, 2015)
    year_2016 = read_file(2016, district, sub_district)
    cleaned_2016 = clean_table(year_2016, district, sub_district, 2016)
    year_2017 = read_file(2017, district, sub_district)
    cleaned_2017 = clean_table(year_2017, district, sub_district, 2017)

    total_years = [cleaned_2012, cleaned_2013, cleaned_2014, cleaned_2015, cleaned_2016, cleaned_2017]
    df = pd.concat(total_years)
    return df
    
    
def fix_column_order(df):
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Year')))
    cols.insert(0, cols.pop(cols.index('District Name')))
    cols.insert(0, cols.pop(cols.index('State Name')))
    
    df = df.reindex(columns= cols)
    return df

def create_state_csv(state_name):
    district_names = []
    path = '/Users/stanleyou/india_health_data/data/2012/MonthUpToMarch/' + state_name
    files = os.listdir(path)
    for i in files:
        district_names.append(append_sub_district(state_name, i))
    state = pd.concat(district_names)
    state = fix_column_order(state)
    state.to_csv('/Users/stanleyou/india_health_data/state_csv/' + state_name + '.csv')