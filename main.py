# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

#internal validity: procedure is right
#external validtiy: generalise the findings to the whole US

import pandas as pd
import os
import re
from datetime import datetime
#import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from scipy.spatial import cKDTree
from shapely.geometry import Point


def ckdnearest(gdA, gdB):
    """
    Find the closest geocoorindtes from dataframe in gdA to dataframe gdB and get to know how far they are away from eachother.
    https://gis.stackexchange.com/questions/222315/finding-nearest-point-in-other-geodataframe-using-geopandas
    """
    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    print(nA)
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    print(nB)
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [
            gdA.reset_index(drop=True),
            gdB_nearest,
            pd.Series(dist, name='dist')
        ],
        axis=1)

    return gdf


if __name__ == "__main__":
    """
    Basepath, Important Note: Change this Path and all the other paths should be updated.
    """
    basepath = "G:\Meine Ablage\Master_Data_Science\Semester 2\Data Science in Heallthcare"

    """
    Load daily Covid cases per counties (total = arround 1900 counties)
    """
    df_covid_counties = pd.read_csv(os.path.join(basepath, r"us_counties_covid19_daily.csv"),sep = ";")
    print(df_covid_counties)
    #print("number of counties in covid dataset ", len(list(set(df_covid_counties["county"].tolist()))))

    """
    Load city mapping table
    """
    df_city_mapping = pd.read_csv(os.path.join(basepath, "us_cities_mapping.csv"),sep = ";")

    """
    Load Temperature city info mapping table with geocordinates
    """
    df_city_info = pd.read_csv(os.path.join(basepath, r"temperature_data\city_info.csv"),sep = ",")
    df_city_info = df_city_info.where(df_city_info["Stn.edDate"]== "2021-12-31").dropna()
    df_city_info = df_city_info.drop(columns=['Unnamed: 0'])
    print(df_city_info)
    #-----------------------------------------------------------------------------------------------------------------------------------------

    """
    Load Temperatures merged
    """
    df_all_temp_city = pd.read_csv(os.path.join(basepath,"temperature_stations_merged_cities.csv"), index_col=0)

    """
    Load Population Density
    """
    df_population = pd.read_csv(os.path.join(basepath,r"population/Average_Household_Size_and_Population_Density_-_County-2018.csv"), sep= ";")
    df_population_density = df_population[["B01001_calc_PopDensity", "NAME", "State"]]
    df_population_density = df_population_density.replace(' County', '', regex=True)
    df_population_density = df_population_density.replace(' Parish', '', regex=True)
    df_population_density = df_population_density.replace(' City', '', regex=True)
    df_population_density = df_population_density.replace(' city', '', regex=True)
    df_population_density.rename(columns = {"NAME": "county", "State": "state"}, inplace=True)
    #df_population_density["NAME"].apply(lambda x: x.split(" ")[0])
    df_population_density.to_csv(os.path.join(basepath, "population_density_cleaned.csv"))

    """
    Load Housing Units
    """
    df_housing_units = pd.read_csv(os.path.join(basepath,r"population/ACSDP5Y2020.DP05_data_with_overlays_2022-03-18T082305.csv"), sep= ";")
    df_housing_units = df_housing_units[["Estimate!!Total housing units", "Geographic Area Name"]]
    df_housing_units = df_housing_units.replace(' County', '', regex=True)
    df_housing_units = df_housing_units.replace(' Parish', '', regex=True)
    df_housing_units = df_housing_units.replace(' City', '', regex=True)
    df_housing_units = df_housing_units.replace(' city', '', regex=True)
    df_housing_units.rename(columns = {"Geographic Area Name": "county"}, inplace=True)
    df_housing_units["state"]= df_housing_units["county"].apply(lambda x: x.split(", ")[1])
    df_housing_units["county"]= df_housing_units["county"].apply(lambda x: x.split(", ")[0])
    df_housing_units.to_csv(os.path.join(basepath,"population_density_cleaned.csv"))


    """
    Geo Mapping with Geo-Pandas
    """
    if os.path.exists(os.path.join(basepath, "geomapping.csv")):
        df_geomapping = pd.read_csv(os.path.join(basepath, "geomapping.csv"), sep=";", index_col=0)
    else:
        #df_city_info[['Lat', 'C']] = df_city_info[['A', 'C']].apply(pd.to_numeric)
        df_city_info = df_city_info.astype({"Lat":float})
        df_city_info = df_city_info.astype({ "Lon":float})
        df_city_mapping = df_city_mapping.astype({"lng":float})
        df_city_mapping = df_city_mapping.astype({ "lat":float})

        gpd_temperatures =gpd.GeoDataFrame(
            df_city_info[["Name","Lat", "Lon" ]], geometry=gpd.points_from_xy(df_city_info["Lon"], df_city_info["Lat"]))

        gpd_city_mapping = gpd.GeoDataFrame(
            df_city_mapping[["city", "lat", "lng", "county_name", "state_name"]], geometry=gpd.points_from_xy(df_city_mapping["lng"], df_city_mapping["lat"]))

        df_geomapping = ckdnearest(gpd_temperatures, gpd_city_mapping)
        df_geomapping.to_csv(os.path.join(basepath, "geomapping.csv"))

        #next step!!! map temperatures of cities in columns to state temperatures

    """
    Create Dictionary of pairs
    """
    if os.path.exists(os.path.join(basepath, "temperatures_counties.csv")):
        df_temp_counties = pd.read_csv(os.path.join(basepath, "temperatures_counties.csv"))
        dict_pairs = {}
        for unique_county in df_geomapping["county_name"].unique():
            dict_pairs.update({unique_county: df_geomapping["Name"].where(
                df_geomapping["county_name"] == unique_county).dropna().tolist()})
    else:
        dict_pairs = {}
        df_temp_counties = pd.DataFrame()
        for unique_county in df_geomapping["county_name"].unique():
            dict_pairs.update({unique_county:df_geomapping["Name"].where(df_geomapping["county_name"]== unique_county).dropna().tolist()})
            for prefix in ["tmax", "tmin", "tmean", "prcp"]:
                lst_prefix_city = [prefix+"_"+i for i in df_geomapping["Name"].where(df_geomapping["county_name"]== unique_county).dropna().tolist()]
                df_county = df_all_temp_city[lst_prefix_city].mean(axis = 1)
                df_county.name = prefix+"_"+unique_county
                df_temp_counties = pd.concat([df_temp_counties, df_county], axis = 1)
                print(df_temp_counties)
        df_temp_counties.to_csv(os.path.join(basepath, "temperatures_counties.csv"))

    df_temp_counties.set_index(df_temp_counties.columns[0], inplace=True)
    print(dict_pairs)

    #def str_to_datetime(date):
    #    return datetime.strptime(date,"%d.%m.%Y")

    """
    Add 0 values for covid deaths and cases in the covid dataset where we have a temperature value but no covid cases registered.
    """
    if os.path.exists(os.path.join(basepath, "covid_cases_counties_cleaned.csv")):
        df_covid_counties = pd.read_csv(os.path.join(basepath, "covid_cases_counties_cleaned.csv"), index_col = 0)
    else:
        unique_dates = list(set(df_covid_counties["date"].tolist()))#.sort(key = str_to_datetime)
        for date in list(set(df_covid_counties["date"].tolist())):
            lst_diff = list( set(list(dict_pairs.keys())) - set(df_covid_counties["county"].where(df_covid_counties["date"]==date)) )
            for county in lst_diff:
                state_name = df_geomapping["state_name"].where(df_geomapping["county_name"]==county).dropna().tolist()[0]
                print(state_name)
                df_covid_counties = df_covid_counties.append({'date': date,"county": county ,"state": state_name, "cases": 0, "deaths": 0}, ignore_index=True)
                print("before_sorting: ", df_covid_counties)

        #Sort values by date and then by county
        df_covid_counties['date'] = pd.to_datetime(df_covid_counties['date'], dayfirst=True)
        df_covid_counties = df_covid_counties.sort_values(['date', 'county'], ascending=True)
        print("after_sorting: ", df_covid_counties)
        df_covid_counties.to_csv(os.path.join(basepath, "covid_cases_counties_cleaned.csv"))



    """
    Beweis Mittelwert der Cities to Counties
    """
    print(df_all_temp_city[["tmax_GreenBay", "tmax_Aberdeen"]].loc["2020-01-01": "2020-01-04"])
    print(df_temp_counties["tmax_Brown"].loc["2020-01-01":"2020-01-04"])

    """
    Transform Temperatures in Long Format
    """
    if os.path.exists(os.path.join(basepath, "temperature_long.csv")):
        df_all_temp_long = pd.read_csv(os.path.join(basepath, "temperature_long.csv"), index_col=0)
    else:
        df_covid_counties.date = df_covid_counties.date.apply(lambda date: datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y"))
        df_all_temp_long = pd.DataFrame()
        for date, county in zip(df_covid_counties["date"],df_covid_counties["county"] ):
            if county in list(dict_pairs.keys()):
                df_temp_long = pd.DataFrame(df_temp_counties[["tmax_"+county, "tmin_"+county, "tmean_"+county, "prcp_"+county]].loc[datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d")]).transpose()
                df_temp_long["county"] = [county]
                #print(df_temp_long)
                df_temp_long.rename(columns = {"tmax_"+county: "tmax", "tmin_"+county: "tmin", "tmean_"+county:"tmean" , "prcp_"+county: "prcp"}, inplace=True)
                df_temp_long.index = [date]
                df_all_temp_long = pd.concat([df_all_temp_long, df_temp_long], axis = 0)
                print(df_all_temp_long)
        df_all_temp_long.to_csv(os.path.join(basepath, "temperature_long.csv"))


    df_all_temp_long["date"] = df_all_temp_long.index
    df_all_temp_long = df_all_temp_long

    """
    Merge Temperatures and COVID Cases
    """
    df_covid_counties.date = df_covid_counties.date.apply(lambda date: datetime.strptime(date,"%Y-%m-%d").strftime("%d.%m.%Y"))
    df_covid_temp_merged = pd.merge(df_covid_counties, df_all_temp_long, on=['date', 'county'], how='left')
    df_covid_temp_merged_dropped = df_covid_temp_merged.drop_duplicates()
    df_covid_temp_merged_dropped.to_csv(os.path.join(basepath, "covid_temperature_merged.csv"))
    df_covid_temp_merged_dropped = df_covid_temp_merged_dropped.dropna(subset=["tmax"])

    df_covid_temp_density = pd.merge(df_covid_temp_merged_dropped, df_population_density, on =["county","state"]  ,how='left').drop_duplicates()
    df_covid_temp_density_housing = pd.merge(df_covid_temp_density, df_housing_units, on =["county","state"] ,how='left').drop_duplicates()
    df_covid_temp_density_housing.to_csv(os.path.join(basepath, "covid_temp_density_housing_without_parish_city.csv"))
    #.where(df_covid_temperatures_merged_dropped["fips"].isna() == False).dropna(subset= ["tmax"])

