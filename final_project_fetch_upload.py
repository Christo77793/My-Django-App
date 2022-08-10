# #!/usr/bin/env python
# # coding: utf-8



#import matplotlib.pyplot as plt
import datetime
from collections import defaultdict
import json,pdb,time,requests,os,sys,datetime
import pandas as pd
from requests.structures import CaseInsensitiveDict
from time import sleep
class data_fetch_upload:
    
   
    def __init__(self):
        self.running_time = time.strftime('%Y-%m-%d-%H')
        self.log_file = open("log_file_"+self.running_time+".log","w+")
        self.credentials_json = open("credentials.json","r")
        self.f = open("api_data"+self.running_time,"w+")
        self.csv_time = time.strftime('%Y-%m-%d-%H-%M')
        self.states_dataframe = pd.DataFrame()
        self.jdata={}
    
    def hit_api(self):
        try:
            
             # hit the api
            headers = CaseInsensitiveDict()
            headers["Authorization"] = "Basic bmlrVmFzaGlzaHQ6TmlraWxlc2g5OA=="
            url = "https://opensky-network.org/api/states/all"
            resp = requests.get(url,headers=headers)
            columns = ["icao24","callsign","origin_country","time_position","last_contact","longitude","latitude","baro_altitude","on_ground","velocity","true_track","vertical_rate","sensors","geo_altitude","squawk","spi","position_source"]
            if resp.status_code == 200: #check if the status is 200 which is success
                self.jdata = resp.json()
                #take the json from api and write to a dataframe

                self.states_dataframe = pd.DataFrame(self.jdata.get("states"), columns=columns)

                # f = open("api_data"+running_time,"w+")
                self.f.write(json.dumps(self.jdata))
                self.log_file.write("api data present and written to a json file\n")
                resp.raise_for_status()
                print("api hit success")
            else:
                print("blocked with: "+str(resp.status_code))
        except requests.exceptions.HTTPError as err:
            print("blocked")
            self.log_file.write("\n"+str(err))
            raise SystemExit(err)


   


    def clean_data(self):
        self.states_dataframe['epoch_time'] = self.jdata.get("time")
        self.log_file.write("taking epoch time\n")
        self.states_dataframe.head()

        #convert epoch time to readable date format
        date_df = []#take date
        time_df = []#take time
        for dates in self.states_dataframe.epoch_time:
            time_df.append(time.strftime('%H:%M:%S', time.localtime(dates)))
            date_df.append(time.strftime('%Y-%m-%d', time.localtime(dates)))
        self.states_dataframe['create_date'] = date_df
        self.states_dataframe['create_time'] = time_df
        self.states_dataframe = self.states_dataframe.drop(columns = ['sensors'])
        # self.states_dataframe.head()
        self.log_file.write("converted epoch time and dropped the column sensors\n")

        self.states_dataframe.to_csv("api_data_"+self.csv_time+".csv",encoding="utf-8",index=False)
        self.log_file.write("writing a csv file\n")
        
        #removing nan values to default as zero
        
        print(self.states_dataframe.head())
        self.states_dataframe['baro_altitude'] = self.states_dataframe['baro_altitude'].fillna(0) 
        self.states_dataframe['time_position'] = self.states_dataframe['time_position'].fillna(0) 
        self.states_dataframe['longitude'] = self.states_dataframe['longitude'].fillna(0)
        self.states_dataframe['latitude'] = self.states_dataframe['latitude'].fillna(0)
        self.states_dataframe['baro_altitude'] = self.states_dataframe['baro_altitude'].fillna(0)
        self.states_dataframe['vertical_rate'] = self.states_dataframe['vertical_rate'].fillna(0)
        self.states_dataframe['geo_altitude'] = self.states_dataframe['geo_altitude'].fillna(0)
        self.states_dataframe['squawk'] = self.states_dataframe['squawk'].fillna(0)
        self.states_dataframe['velocity'] = self.states_dataframe['velocity'].fillna(0)
        
        
    def connect_cursor(self):
        #uploading to azure sql data base
        self.log_file.write("connecting the cursor\n")

        credentials_json_data = json.load(self.credentials_json)
        import pyodbc
        server = credentials_json_data.get("server")
        database = credentials_json_data.get("database")
        username = credentials_json_data.get("username")
        password = credentials_json_data.get("password")
        driver= '{'+credentials_json_data.get("drive",'ODBC Driver 17 for SQL Server')+'}'

        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            # with conn.cursor() as cursor:
            #     cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
            #     row = cursor.fetchone()
            insert_to_tmp_tbl_stmt = f"INSERT INTO ATC_Details ([icao24], [callsign], [origin_country], [time_position], [last_contact], [longitude], [latitude], [baro_altitude], [on_ground], [velocity], [true_track], [vertical_rate],  [geo_altitude], [squawk], [position], [spi], [epoch_time], [create_date], [create_time]) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor = conn.cursor()  #create a cursor
            start = time.time()     #Note start time
            cursor.executemany(insert_to_tmp_tbl_stmt, self.states_dataframe.values.tolist()) #load data into azure sql db
            end = time.time()       #Note end time

            self.log_file.write(f'{len(self.states_dataframe)} rows inserted in ATC table\n')
            self.log_file.write(f'{(end - start)/60} minutes elapsed\n')            

            cursor.commit()       #Close the cursor and connection
 
    def upload_blob(self):
    #uploading to azure blob
        self.log_file.write("uploading to azure folder\n")
        try:
            from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
            connection_string = credentials_json_data.get("connection_string")
            # blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=dpfinal;AccountKey=ONyBWjswEzodfTI6xh0JnSWmdEQBngw70dgJanWoMHMbII4YDJdzddg+IximnASUs44w2HcsWRSy+ASty1vXfA==;EndpointSuffix=core.windows.net")
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container = 'dp-storage', blob = "api_data_"+self.csv_time+".csv")
            print("\nUploading to Azure Storage as blob:\n\t")
            with open("api_data_"+self.csv_time+".csv", "rb") as data:
                blob_client.upload_blob(data)
            blob_client = blob_service_client.get_blob_client(container = 'dp-storage', blob = "log_file_"+self.running_time+".log")

            with open("log_file_"+self.running_time+".log","rb") as data:
                blob_client.upload_blob(data)

            blob_client = blob_service_client.get_blob_client(container = 'dp-storage', blob = "api_data"+self.running_time)
            with open("api_data"+self.running_time, "rb") as data:
                blob_client.upload_blob(data)

            print("upload completed")

        except Exception as e:
            print("upload aborted due to: "+str(e))
            self.log_file.write("\n"+str(e))
        self.log_file.close()
        
if __name__ == "__main__":
    try:
        while True:
            reader = data_fetch_upload()
            reader.hit_api()
            reader.clean_data()
            reader.connect_cursor()
            reader.upload_blob()
            time.sleep(86400)
            x = datetime.datetime.now()
            if int(x.strftime("%Y%m%d")) >20220811:
                sys.exit("Job Done")
    except Exception as e:
        print("aborted: "+str(e))