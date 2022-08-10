import os.path

from django.shortcuts import render

import calendar
import pandas as pd
import matplotlib.pyplot as plt, mpld3

from calendar import HTMLCalendar
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
import plotly
import json
import requests as req


# BOOTSTRAP
# https://getbootstrap.com/docs/5.0/getting-started/introduction/

def home(request):
    response = req.get(r'https://atcapi20220809022704.azurewebsites.net/AtcDetails/')
    data = response.text
    df = pd.read_json(data, orient='records')
    fig = px.scatter_polar(theta=df['trueTrack'], r=df['velocity'], template="plotly_dark", color=df['trueTrack'])
    fig.update_layout(autosize=True, hovermode='closest')
    g1json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    fig1 = px.density_mapbox(lat=df['latitude'], lon=df['longitude'], radius=1, mapbox_style='stamen-terrain', zoom=0.75, center=dict(lat=0, lon=0))
    g2json = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

    dfg = df.groupby(['originCountry']).size().to_frame().sort_values([0], ascending=False).head(10).reset_index()
    dfg.columns = ['originCountry', 'count']
    fig4 = px.bar(dfg, x='originCountry', y='count', hover_data=["originCountry", "count"])

    fig4.layout.yaxis.title.text = 'Count'
    fig4.layout.xaxis.title.text = 'Origin Country'

    fig4.update_traces(marker_color='green')

    # fig.update_layout(width=800, height=500,autosize= F)

    fig4.update_layout(autosize=False, width=800, height=600, )
    g3json = json.dumps(fig4, cls=plotly.utils.PlotlyJSONEncoder)

    create_date = df['createDate'].unique()
    create_date = create_date[0][0:10]
    create_time = df['createTime'].unique()
    create_time = create_time[0]

    return render(
        request,
        "app1/home.html",
        {
            "g1json": g1json,
            "g2json": g2json,
            "g3json": g3json,
            "createDate": create_date,
            "createTime": create_time,
        }
    )
