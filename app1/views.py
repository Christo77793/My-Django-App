import os.path

from django.shortcuts import render

import calendar
import pandas as pd
import matplotlib.pyplot as plt, mpld3

from calendar import HTMLCalendar
from datetime import datetime


# BOOTSTRAP
# https://getbootstrap.com/docs/5.0/getting-started/introduction/

def home(request):
    project_name = "Data Programming Final Project"

    df = pd.read_csv('https://raw.githubusercontent.com/Christo77793/Data-Sets/master/api_data.csv')

    df1 = pd.Series(df['velocity'].values, index=df['true_track']).to_dict()
    dfx = pd.Series(df['velocity'].values, index=df['true_track'])
    df2 = pd.Series(df['longitude'].values, index=df['latitude']).to_dict()

    data1 = {}
    data1.update(dfx.head(10))

    data2 = {"Longitude": "Latitude"}
    data2.update(df2)

    return render(
        request,
        "app1/home.html",

        {
            "project_name": project_name,
            "data1": data1,
            "data2": data2,
        }

    )
