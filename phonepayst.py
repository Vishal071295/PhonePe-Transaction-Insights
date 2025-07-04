#pakages
import os
import json
import git
import pandas as pd

#aggre_transaction
path1 = "D:/PHONEPAY/pulse/data/aggregated/transaction/country/india/state/"
         
agg_tran_list = os.listdir(path1)

columns1 ={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }

for state in agg_tran_list:
    cur_states =path1+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)

            for i in B["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

aggre_transaction = pd.DataFrame(columns1)

aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#aggre_insurance
path2= "D:/PHONEPAY/pulse/data/aggregated/insurance/country/india/state/"

agg_insur_list= os.listdir(path2)

columns2= {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }

for state in agg_insur_list:
    cur_states =path2+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns2["Insurance_type"].append(name)
                columns2["Insurance_count"].append(count)
                columns2["Insurance_amount"].append(amount)
                columns2["States"].append(state)
                columns2["Years"].append(year)
                columns2["Quarter"].append(int(file.strip(".json")))


aggre_insurance = pd.DataFrame(columns2)

aggre_insurance["States"] = aggre_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_insurance["States"] = aggre_insurance["States"].str.replace("-"," ")
aggre_insurance["States"] = aggre_insurance["States"].str.title()
aggre_insurance['States'] = aggre_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#aggre_user
path3 = "D:/PHONEPAY/pulse/data/aggregated/user/country/india/state/"
         
agg_user_list = os.listdir(path3)

columns3 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

for state in agg_user_list:
    cur_states = path3+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            try:

                for i in C["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns3["Brands"].append(brand)
                    columns3["Transaction_count"].append(count)
                    columns3["Percentage"].append(percentage)
                    columns3["States"].append(state)
                    columns3["Years"].append(year)
                    columns3["Quarter"].append(int(file.strip(".json")))
            
            except:
                pass

aggre_user = pd.DataFrame(columns3)

aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#map_transaction
path4 = "D:/PHONEPAY/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list = os.listdir(path4)

columns4 = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}

for state in map_tran_list:
    cur_states = path4+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            E = json.load(data)

            for i in E['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns4["District"].append(name)
                columns4["Transaction_count"].append(count)
                columns4["Transaction_amount"].append(amount)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(columns4)

map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#map_insurance
path5= "D:/PHONEPAY/pulse/data/map/insurance/hover/country/india/state/"

map_insur_list= os.listdir(path5)

columns5= {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[],"Transaction_amount":[] }

for state in map_insur_list:
    cur_states =path5+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns5["Districts"].append(name)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))


map_insurance = pd.DataFrame(columns5)

map_insurance["States"] = map_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_insurance["States"] = map_insurance["States"].str.replace("-"," ")
map_insurance["States"] = map_insurance["States"].str.title()
map_insurance['States'] = map_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#map_user
path6 = "D:/PHONEPAY/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path6)

columns6 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user_list:
    cur_states = path6+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)

            for i in F["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns6["Districts"].append(district)
                columns6["RegisteredUser"].append(registereduser)
                columns6["AppOpens"].append(appopens)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(columns6)

map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#top_transaction
path7 = "D:/PHONEPAY/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path5)

columns7 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_tran_list:
    cur_states = path7+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns7["Pincodes"].append(entityName)
                columns7["Transaction_count"].append(count)
                columns7["Transaction_amount"].append(amount)
                columns7["States"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(columns7)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#top_insurance

path8 = "D:/PHONEPAY/pulse/data/top/insurance/country/india/state/"

top_insur_list = os.listdir(path8)

columns8 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_insur_list:
    cur_states = path8+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            G = json.load(data)

            for i in G["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns8["Pincodes"].append(entityName)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["States"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))

top_insur = pd.DataFrame(columns8)

top_insur["States"] = top_insur["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_insur["States"] = top_insur["States"].str.replace("-"," ")
top_insur["States"] = top_insur["States"].str.title()
top_insur['States'] = top_insur['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#top_user
path9 = "D:/PHONEPAY/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path9)

columns9 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_list:
    cur_states = path9+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            I = json.load(data)

            for i in I["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns9["Pincodes"].append(name)
                columns9["RegisteredUser"].append(registereduser)
                columns9["States"].append(state)
                columns9["Years"].append(year)
                columns9["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(columns9)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

import mysql.connector
mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
cursor = mydb.cursor()

cursor.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_user(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Brands VARCHAR(50),
                    Transaction_count BIGINT,
                    Percentage FLOAT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_transaction(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Transaction_type VARCHAR(50),
                    Transaction_count BIGINT,
                    Transaction_amount BIGINT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_insurance(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Insurance_type VARCHAR(50),
                    Insurance_count BIGINT,
                    Insurance_amount BIGINT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS map_insurance(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    District VARCHAR(50),
                    Transaction_count BIGINT,
                    Transaction_amount FLOAT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS map_user(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    District VARCHAR(50),
                    RegisteredUser BIGINT,
                    AppOpens BIGINT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS map_transaction(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    District VARCHAR(50),
                    Transaction_count BIGINT,
                    Transaction_amount FLOAT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_insurance(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Pincodes INT,
                    Transaction_count BIGINT,
                    Transaction_amount BIGINT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_transaction(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Pincodes INT,
                    Transaction_count BIGINT,
                    Transaction_amount BIGINT)
        """)

cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_user(
            States VARCHAR(50),
                    Years INT,
                    Quarter INT,
                    Pincodes INT,
                    RegisteredUser BIGINT)
        """)


query = "INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in aggre_user.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in aggre_transaction.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in aggre_insurance.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO map_insurance (States, Years, Quarter, District, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in map_insurance.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO map_user (States, Years, Quarter, District, RegisteredUser, AppOpens) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in map_user.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO map_transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in map_transaction.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in top_insur.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
values = [tuple(x) for x in top_transaction.to_numpy()]

cursor.executemany(query, values)

query = "INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser) VALUES (%s, %s, %s, %s, %s)"
values = [tuple(x) for x in top_user.to_numpy()]

cursor.executemany(query, values)

import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
import pandas as pd
import plotly.express as px
import requests
import json
from PIL import Image

# Dataframe Creation

import mysql.connector
mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
cursor = mydb.cursor()

#aggre_insurance_df
cursor.execute("SELECT * FROM aggregated_insurance")    
table1= cursor.fetchall()

Aggre_insurance= pd.DataFrame(table1, columns=("States", "Years", "Quarter", "Transaction_type",
                                               "Transaction_count", "Transaction_amount"))

#aggre_transaction_df
cursor.execute("SELECT * FROM aggregated_transaction")
table2= cursor.fetchall()

Aggre_transaction= pd.DataFrame(table2, columns=("States", "Years", "Quarter", "Transaction_type",
                                               "Transaction_count", "Transaction_amount"))

#aggre_user_df
cursor.execute("SELECT * FROM aggregated_user")
table3= cursor.fetchall()

Aggre_user= pd.DataFrame(table3, columns=("States", "Years", "Quarter", "Brands",
                                               "Transaction_count", "Percentage"))

#map_insurance
cursor.execute("SELECT * FROM map_insurance")
table4= cursor.fetchall()

map_insurance= pd.DataFrame(table4, columns=("States", "Years", "Quarter", "District",
                                               "Transaction_count", "Transaction_amount"))

#map_transction
cursor.execute("SELECT * FROM map_transaction")
table5= cursor.fetchall()

map_transaction= pd.DataFrame(table5, columns=("States", "Years", "Quarter", "District",
                                               "Transaction_count", "Transaction_amount"))

#map_user
cursor.execute("SELECT * FROM map_user")
table6= cursor.fetchall()

map_user= pd.DataFrame(table6, columns=("States", "Years", "Quarter", "District",
                                               "RegisteredUser", "AppOpens"))

#top_insurance
cursor.execute("SELECT * FROM top_insurance")
table7= cursor.fetchall()

top_insurance= pd.DataFrame(table7, columns=("States", "Years", "Quarter", "Pincodes",
                                               "Transaction_count", "Transaction_amount"))

#top_transaction
cursor.execute("SELECT * FROM top_transaction")
table8= cursor.fetchall()

top_transaction= pd.DataFrame(table8, columns=("States", "Years", "Quarter", "Pincodes",
                                               "Transaction_count", "Transaction_amount"))

#top_user
cursor.execute("SELECT * FROM top_user")
table9= cursor.fetchall()

top_user= pd.DataFrame(table9, columns=("States", "Years", "Quarter", "Pincodes",
                                               "RegisteredUsers"))   


def Transaction_amount_count_Y(df, year):

    tacy= df[df["Years"] == year]
    tacy.reset_index(drop = True, inplace= True)

    tacyg= tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)


    col1,col2 = st.columns(2)
    with col1:

        fig_amount1= px.bar(tacyg, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount1)

    with col2:
        fig_count1= px.bar(tacyg, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 650, width= 600)
        st.plotly_chart(fig_count1)


    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name= []
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()

        fig_india_1= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_1.update_geos(visible= False)
        st.plotly_chart(fig_india_1)

    with col2:

        fig_india_2= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION COUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_2.update_geos(visible= False)
        st.plotly_chart(fig_india_2)

    return tacy

def Transaction_amount_count_Y_Q(df, quarter):
    tacy= df[df["Quarter"] == quarter]
    tacy.reset_index(drop = True, inplace= True)

    tacyg= tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:

        fig_amount_t= px.bar(tacyg, x="States", y="Transaction_amount", title=f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount_t)

    with col2:
        fig_count_t= px.bar(tacyg, x="States", y="Transaction_count", title=f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 650,width= 600)
        st.plotly_chart(fig_count_t)

    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name= []
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()

        fig_india_11= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_11.update_geos(visible= False)
        st.plotly_chart(fig_india_11)
    
    with col2:

        fig_india_22= px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color= "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT", fitbounds= "locations",
                                height= 600,width= 600)
        fig_india_22.update_geos(visible= False)
        st.plotly_chart(fig_india_22)

    return tacy

            
def Aggre_Tran_Transaction_type(df, state):

    tacy= df[df["States"] == state]
    tacy.reset_index(drop = True, inplace= True)

    tacyg= tacy.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_pie_1= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_amount",
                            width= 600, title= f"{state.upper()} TRANSACTION AMOUNT", hole= 0.5)
        st.plotly_chart(fig_pie_1)

    with col2:
        fig_pie_2= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_count",
                            width= 600, title= f"{state.upper()} TRANSACTION COUNT", hole= 0.5)
        st.plotly_chart(fig_pie_2)


# Aggre_User_analysis_1
def Aggre_user_plot_1(df, year):
    aguy= df[df["Years"]== year]
    aguy.reset_index(drop= True, inplace= True)

    aguyg= pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace= True)

    fig_bar_1= px.bar(aguyg, x= "Brands", y= "Transaction_count", title= f"{year} BRANDS AND TRANSACTION COUNT",
                    width= 1000, color_discrete_sequence= px.colors.sequential.haline_r, hover_name= "Brands")
    st.plotly_chart(fig_bar_1)

    return aguy

#Aggre_user_Analysis_2
def Aggre_user_plot_2(df, quarter):
    aguyq= df[df["Quarter"]== quarter]
    aguyq.reset_index(drop= True, inplace= True)

    aguyqg= pd.DataFrame(aguyq.groupby("Brands")["Transaction_count"].sum())
    aguyqg.reset_index(inplace= True)

    fig_bar_2= px.bar(aguyqg, x= "Brands", y= "Transaction_count", title=  f"{quarter} QUARTER, BRANDS AND TRANSACTION COUNT",
                    width= 1000, color_discrete_sequence= px.colors.sequential.Magenta_r, hover_name="Brands")
    st.plotly_chart(fig_bar_2)

    return aguyq


#Aggre_user_alalysis_3
def Aggre_user_plot_3(df, state):
    auyqs= df[df["States"] == state]
    auyqs.reset_index(drop= True, inplace= True)

    fig_line_1= px.line(auyqs, x= "Brands", y= "Transaction_count", hover_data= "Percentage",
                        title= f"{state.upper()} BRANDS, TRANSACTION COUNT, PERCENTAGE",width= 1000, markers= True)
    st.plotly_chart(fig_line_1)


#Map_insurance_district
def Map_insur_District(df, state):

    tacy= df[df["States"] == state]
    tacy.reset_index(drop = True, inplace= True)

    tacyg= tacy.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_bar_11= px.bar(tacyg, x= "Transaction_amount", y= "District", orientation= "h", height= 600,
                        title= f"{state.upper()} DISTRICT AND TRANSACTION AMOUNT", color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_bar_11)

    with col2:

        fig_bar_22= px.bar(tacyg, x= "Transaction_count", y= "District", orientation= "h", height= 600,
                        title= f"{state.upper()} DISTRICT AND TRANSACTION COUNT", color_discrete_sequence= px.colors.sequential.Bluered_r)
        st.plotly_chart(fig_bar_22)

# map_user_plot_1
def map_user_plot_1(df, year):
    muy= df[df["Years"]== year]
    muy.reset_index(drop= True, inplace= True)

    muyg= muy.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    muyg.reset_index(inplace= True)

    fig_line_11= px.line(muyg, x= "States", y= ["RegisteredUser", "AppOpens"],
                        title= f"{year} REGISTERED USER, APPOPENS",width= 1000, height= 800, markers= True)
    st.plotly_chart(fig_line_11)

    return muy

# map_user_plot_2
def map_user_plot_2(df, quarter):
    muyq= df[df["Quarter"]== quarter]
    muyq.reset_index(drop= True, inplace= True)

    muyqg= muyq.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    muyqg.reset_index(inplace= True)

    fig_line_111= px.line(muyqg, x= "States", y= ["RegisteredUser", "AppOpens"],
                        title= f"{df['Years'].min()} YEARS {quarter} QUARTER REGISTERED USER, APPOPENS",width= 1000, height= 800, markers= True,
                        color_discrete_sequence= px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_line_111)

    return muyq

#map_user_plot_3
def map_user_plot_3(df, states):
    muyqs= df[df["States"]== states]
    muyqs.reset_index(drop= True, inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_user_bar_1= px.bar(muyqs, x= "RegisteredUser", y= "District", orientation= "h",
                                title= f"{states.upper()} REGISTERED USER", height= 800, color_discrete_sequence= px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_map_user_bar_1)

    with col2:

        fig_map_user_bar_2= px.bar(muyqs, x= "AppOpens", y= "District", orientation= "h",
                                title= f"{states.upper()} APPOPENS", height= 800, color_discrete_sequence= px.colors.sequential.Rainbow)
        st.plotly_chart(fig_map_user_bar_2)

# top_insurance_plot_1
def Top_insurance_plot_1(df, state):
    tiy= df[df["States"]== state]
    tiy.reset_index(drop= True, inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_top_insur_bar_1= px.bar(tiy, x= "Quarter", y= "Transaction_amount", hover_data= "Pincodes",
                                title= "TRANSACTION AMOUNT", height= 650,width= 600, color_discrete_sequence= px.colors.sequential.GnBu_r)
        st.plotly_chart(fig_top_insur_bar_1)

    with col2:

        fig_top_insur_bar_2= px.bar(tiy, x= "Quarter", y= "Transaction_count", hover_data= "Pincodes",
                                title= "TRANSACTION COUNT", height= 650,width= 600, color_discrete_sequence= px.colors.sequential.Agsunset_r)
        st.plotly_chart(fig_top_insur_bar_2)

def top_user_plot_1(df, year):
    tuy= df[df["Years"]== year]
    tuy.reset_index(drop= True, inplace= True)

    tuyg= pd.DataFrame(tuy.groupby(["States", "Quarter"])["RegisteredUsers"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuyg, x= "States", y= "RegisteredUsers", color= "Quarter", width= 1000, height= 800,
                        color_discrete_sequence= px.colors.sequential.Burgyl, hover_name= "States",
                        title= f"{year} REGISTERED USERS")
    st.plotly_chart(fig_top_plot_1)

    return tuy


# top_user_plot_2
def top_user_plot_2(df, state):
    tuys= df[df["States"]== state]
    tuys.reset_index(drop= True, inplace= True)

    fig_top_pot_2= px.bar(tuys, x= "Quarter", y= "RegisteredUsers", title= "REGISTEREDUSERS, PINCODES, QUARTER",
                        width= 1000, height= 800, color= "RegisteredUsers", hover_data= "Pincodes",
                        color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_pot_2)

#sql connection
def top_chart_transaction_amount(table_name):
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
    cursor = mydb.cursor()

    #plot_1
    query1= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table_1= cursor.fetchall()


    df_1= pd.DataFrame(table_1, columns=("states", "transaction_amount"))

    col1,col2= st.columns(2)
    with col1:

        fig_amounts= px.bar(df_1, x="states", y="transaction_amount", title="TOP 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amounts)

    #plot_2
    query2= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount
                LIMIT 10;'''

    cursor.execute(query2)
    table_2= cursor.fetchall()

    df_2= pd.DataFrame(table_2, columns=("states", "transaction_amount"))
    
    with col2:
        fig_amount_2= px.bar(df_2, x="states", y="transaction_amount", title="LAST 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)

    #plot_3
    query3= f'''SELECT states, AVG(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount;'''

    cursor.execute(query3)
    table_3= cursor.fetchall()

    df_3= pd.DataFrame(table_3, columns=("states", "transaction_amount"))

    fig_amount_3= px.bar(df_3, y="states", x="transaction_amount", title="AVERAGE OF TRANSACTION AMOUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_transaction_count(table_name):
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
    cursor = mydb.cursor()

    #plot_1
    query1= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table_1= cursor.fetchall()
    

    df_1= pd.DataFrame(table_1, columns=("states", "transaction_count"))

    col1,col2= st.columns(2)
    with col1:
        fig_amount= px.bar(df_1, x="states", y="transaction_count", title="TOP 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count
                LIMIT 10;'''

    cursor.execute(query2)
    table_2= cursor.fetchall()
    

    df_2= pd.DataFrame(table_2, columns=("states", "transaction_count"))

    with col2:
        fig_amount_21= px.bar(df_2, x="states", y="transaction_count", title="LAST 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_21)

    #plot_3
    query3= f'''SELECT states, AVG(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count;'''

    cursor.execute(query3)
    table_3= cursor.fetchall()
    

    df_3= pd.DataFrame(table_3, columns=("states", "transaction_count"))

    fig_amount_31= px.bar(df_3, y="states", x="transaction_count", title="AVERAGE OF TRANSACTION COUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_31)



#sql connection
def top_chart_registered_user(table_name, state):
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
    cursor = mydb.cursor()

    #plot_1
    query1= f'''SELECT districts, SUM(registereduser) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table_1= cursor.fetchall()
    

    df_1= pd.DataFrame(table_1, columns=("districts", "registereduser"))

    col1,col2= st.columns(2)
    with col1:
        fig_amountt= px.bar(df_1, x="districts", y="registereduser", title="TOP 10 OF REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amountt)

    #plot_2
    query2= f'''SELECT districts, SUM(registereduser) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser
                LIMIT 10;'''

    cursor.execute(query2)
    table_2= cursor.fetchall()
   

    df_2= pd.DataFrame(table_2, columns=("districts", "registereduser"))

    with col2:
        fig_amount_22= px.bar(df_2, x="districts", y="registereduser", title="LAST 10 REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_22)

    #plot_3
    query3= f'''SELECT districts, AVG(registereduser) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser;'''

    cursor.execute(query3)
    table_3= cursor.fetchall()
    

    df_3= pd.DataFrame(table_3, columns=("districts", "registereduser"))

    fig_amount_33= px.bar(df_3, y="districts", x="registereduser", title="AVERAGE OF REGISTERED USER", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_33)

#sql connection
def top_chart_appopens(table_name, state):
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
    cursor = mydb.cursor()
    #plot_1
    query1= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table_1= cursor.fetchall()
    

    df_1= pd.DataFrame(table_1, columns=("districts", "appopens"))


    col1,col2= st.columns(2)
    with col1:

        fig_amount_6= px.bar(df_1, x="districts", y="appopens", title="TOP 10 OF APPOPENS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount_6)

    #plot_2
    query2= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens
                LIMIT 10;'''

    cursor.execute(query2)
    table_2= cursor.fetchall()
   

    df_2= pd.DataFrame(table_2, columns=("districts", "appopens"))

    with col2:

        fig_amount_7= px.bar(df_2, x="districts", y="appopens", title="LAST 10 APPOPENS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_7)

    #plot_3
    query3= f'''SELECT districts, AVG(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens;'''

    cursor.execute(query3)
    table_3= cursor.fetchall()
   

    df_3= pd.DataFrame(table_3, columns=("districts", "appopens"))

    fig_amount_8= px.bar(df_3, y="districts", x="appopens", title="AVERAGE OF APPOPENS", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_8)

#sql connection
def top_chart_registered_users(table_name):
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Vishal@0712",
        database = "phonepay",
        autocommit = True)
    cursor = mydb.cursor()
    #plot_1
    query1= f'''SELECT states, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table_1= cursor.fetchall()
    
    df_1= pd.DataFrame(table_1, columns=("states", "registeredusers"))
    
    col1,col2= st.columns(2)
    with col1:

        fig_amount_9= px.bar(df_1, x="states", y="registeredusers", title="TOP 10 OF REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount_9)

    #plot_2
    query2= f'''SELECT states, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers
                LIMIT 10;'''

    cursor.execute(query2)
    table_2= cursor.fetchall()
   

    df_2= pd.DataFrame(table_2, columns=("states", "registeredusers"))

    with col2:

        fig_amount_10= px.bar(df_2, x="states", y="registeredusers", title="LAST 10 REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_10)

    #plot_3
    query3= f'''SELECT states, AVG(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers;'''

    cursor.execute(query3)
    table_3= cursor.fetchall()
    

    df_3= pd.DataFrame(table_3, columns=("states", "registeredusers"))

    fig_amount_12= px.bar(df_3, y="states", x="registeredusers", title="AVERAGE OF REGISTERED USERS", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_12)


# Streamlit Part

st.set_page_config(layout= "wide")
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")

with st.sidebar:
    
    select= option_menu("Main Menu",["HOME", "DATA EXPLORATION", "BUSINESS CASE STUDY"])

if select == "HOME":
    
    col1,col2= st.columns(2)

    with col1:
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    with col2:
        st.image(Image.open(r"D:\PHONEPAY\new1.jpg"),width= 600)

    col3,col4= st.columns(2)
    
    with col3:
        st.image(Image.open(r"D:\PHONEPAY\1.jpg"),width=600)

    with col4:
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    col5,col6= st.columns(2)

    with col5:
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")

    with col6:
        st.image(Image.open(r"D:\PHONEPAY\3.jpg"),width= 600)


elif select == "DATA EXPLORATION":

    tab1, tab2, tab3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:

        method = st.radio("Select The Method",["Insurance Analysis", "Transaction Analysis", "User Analysis"])

        if method == "Insurance Analysis":

            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",Aggre_insurance["Years"].min(), Aggre_insurance["Years"].max(),Aggre_insurance["Years"].min())
            tac_Y= Transaction_amount_count_Y(Aggre_insurance, years)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter",tac_Y["Quarter"].min(), tac_Y["Quarter"].max(),tac_Y["Quarter"].min())
            Transaction_amount_count_Y_Q(tac_Y, quarters)

        elif method == "Transaction Analysis":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",Aggre_transaction["Years"].min(), Aggre_transaction["Years"].max(),Aggre_transaction["Years"].min())
            Aggre_tran_tac_Y= Transaction_amount_count_Y(Aggre_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State", Aggre_tran_tac_Y["States"].unique())

            Aggre_Tran_Transaction_type(Aggre_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter",Aggre_tran_tac_Y["Quarter"].min(), Aggre_tran_tac_Y["Quarter"].max(),Aggre_tran_tac_Y["Quarter"].min())
            Aggre_tran_tac_Y_Q= Transaction_amount_count_Y_Q(Aggre_tran_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_Ty", Aggre_tran_tac_Y_Q["States"].unique())

            Aggre_Tran_Transaction_type(Aggre_tran_tac_Y_Q, states)

        elif method == "User Analysis":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",Aggre_user["Years"].min(), Aggre_user["Years"].max(),Aggre_user["Years"].min())
            Aggre_user_Y= Aggre_user_plot_1(Aggre_user, years)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter",Aggre_user_Y["Quarter"].min(), Aggre_user_Y["Quarter"].max(),Aggre_user_Y["Quarter"].min())
            Aggre_user_Y_Q= Aggre_user_plot_2(Aggre_user_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State", Aggre_user_Y_Q["States"].unique())

            Aggre_user_plot_3(Aggre_user_Y_Q, states)




    with tab2:

        method_2= st.radio("Select The Method",["Map Insurance", "Map Transaction", "Map User"])

        if method_2 == "Map Insurance":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year_mi",map_insurance["Years"].min(), map_insurance["Years"].max(),map_insurance["Years"].min())
            map_insur_tac_Y= Transaction_amount_count_Y(map_insurance, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_mi", map_insur_tac_Y["States"].unique())

            Map_insur_District(map_insur_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter_mi",map_insur_tac_Y["Quarter"].min(), map_insur_tac_Y["Quarter"].max(),map_insur_tac_Y["Quarter"].min())
            map_insur_tac_Y_Q= Transaction_amount_count_Y_Q(map_insur_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_Ty", map_insur_tac_Y_Q["States"].unique())

            Map_insur_District(map_insur_tac_Y_Q, states)

        elif method_2 == "Map Transaction":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",map_transaction["Years"].min(), map_transaction["Years"].max(),map_transaction["Years"].min())
            map_tran_tac_Y= Transaction_amount_count_Y(map_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_mi", map_tran_tac_Y["States"].unique())

            Map_insur_District(map_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter_mt",map_tran_tac_Y["Quarter"].min(), map_tran_tac_Y["Quarter"].max(),map_tran_tac_Y["Quarter"].min())
            map_tran_tac_Y_Q= Transaction_amount_count_Y_Q(map_tran_tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_Ty", map_tran_tac_Y_Q["States"].unique())

            Map_insur_District(map_tran_tac_Y_Q, states)


        elif method_2 == "Map User":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year_mu",map_user["Years"].min(), map_user["Years"].max(),map_user["Years"].min())
            map_user_Y= map_user_plot_1(map_user, years)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter_mu",map_user_Y["Quarter"].min(), map_user_Y["Quarter"].max(),map_user_Y["Quarter"].min())
            map_user_Y_Q= map_user_plot_2(map_user_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_mu", map_user_Y_Q["States"].unique())

            map_user_plot_3(map_user_Y_Q, states)

    with tab3:

        method_3= st.radio("Select The Method",["Top Insurance", "Top Transaction", "Top User"])

        if method_3 == "Top Insurance":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year_ti",top_insurance["Years"].min(), top_insurance["Years"].max(),top_insurance["Years"].min())
            top_insur_tac_Y= Transaction_amount_count_Y(top_insurance, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_ti", top_insur_tac_Y["States"].unique())

            Top_insurance_plot_1(top_insur_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter_mu",top_insur_tac_Y["Quarter"].min(), top_insur_tac_Y["Quarter"].max(),top_insur_tac_Y["Quarter"].min())
            top_insur_tac_Y_Q= Transaction_amount_count_Y_Q(top_insur_tac_Y, quarters)

            

        elif method_3 == "Top Transaction":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year_tt",top_transaction["Years"].min(), top_transaction["Years"].max(),top_transaction["Years"].min())
            top_tran_tac_Y= Transaction_amount_count_Y(top_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_tt", top_tran_tac_Y["States"].unique())

            Top_insurance_plot_1(top_tran_tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter_tt",top_tran_tac_Y["Quarter"].min(), top_tran_tac_Y["Quarter"].max(),top_tran_tac_Y["Quarter"].min())
            top_tran_tac_Y_Q= Transaction_amount_count_Y_Q(top_tran_tac_Y, quarters)


        elif method_3 == "Top User":
            
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year_tu",top_user["Years"].min(), top_user["Years"].max(),top_user["Years"].min())
            top_user_Y= top_user_plot_1(top_user, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_tu", top_user_Y["States"].unique())

            top_user_plot_2(top_user_Y, states)

elif select == "BUSINESS CASE STUDY":
    
    question= st.selectbox("Select the Question",["1. Transaction Amount and Count of Map Insurance",
                                                    "2. Transaction Amount and Count of Top Insurance",
                                                    "3. Transaction Amount and Count of Aggregated Transaction",
                                                    "4. Transaction Amount and Count of Map Transaction",
                                                    "5. Transaction Amount and Count of Top Transaction",
                                                    "6. Transaction Count of Aggregated User",
                                                    ])
    
    if question == "1. Transaction Amount and Count of Map Insurance":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_insurance")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_insurance")

    elif question == "2. Transaction Amount and Count of Top Insurance":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("top_insurance")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("top_insurance")

    elif question == "3. Transaction Amount and Count of Aggregated Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("aggregated_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_transaction")

    elif question == "4. Transaction Amount and Count of Map Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_transaction")

    elif question == "5. Transaction Amount and Count of Top Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("top_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("top_transaction")

    elif question == "6. Transaction Count of Aggregated User":

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_user")