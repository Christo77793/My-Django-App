from django.shortcuts import render
import calendar
from calendar import HTMLCalendar
from datetime import datetime


# BOOTSTRAP
# https://getbootstrap.com/docs/5.0/getting-started/introduction/

def home(request):
    project_name = "Data Programming Final Project"

    return render(
        request,
        "app1/home.html",
        {
            "project_name": project_name,
        }
    )
