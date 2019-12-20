import json
import requests
import re
import datetime
import sqlite3
import time
import pytemperature
import plotly.express as px
import plotly.graph_objs as go

# pip install ipywidgets
# install plotly
# pip install statsmodels 
# install pytemperature


class API_ACCESS():
    def __init__(self):
        self.url1 = "https://mprint.umich.edu/api/queues/m-ulib-1000-bw-2?detailed"
        self.url2 = "https://mprint.umich.edu/api/queues/m-ulib-2000-bw-1?detailed"
        self.url3 = "http://api.openweathermap.org/data/2.5/weather?q=Ann%20Arbor&APPID=d44ff1cf9d575579f9b65fdaf0f4ccdd"
        self.counter = 0

        # find library or thing that grabs this automatically from url
        self.headers = {
            'Cookie': "um_cookie_consent=na; gwlob=on; cosign-mprint=MmExz8waPWJqiufl2QXekZ-xj3ceQ3gzMrqL4LtdHURqK6TeItUbqCKbJQKXfzpvKXoKFLkvqX8s13MuxTlN-wi3Nv1wAwwgO6JnJns5m8bbdZB6tuBvu65LLgdc/1576807496"
        }
        self.all_jobs = []
        self.db = "SI_FINAL_DB.db"


    def get_weather_data(self):
        ''' Requests data from the OpenWeather API and extracts the necessary information into a tuple'''
        
        r = requests.get(self.url3, headers=self.headers)
        request_dict = json.loads(r.text) # And then load it into a dictionary

        weather = request_dict["weather"][0]["main"].lower()
        weather_id = 0
        temperature = request_dict["main"]["temp"]
        dateTimeObj = datetime.datetime.now()
        current_time_stamp = str(dateTimeObj)

        if (weather == "drizzle"):
            weather_id = 1
        elif (weather == "rain"):
            weather_id = 2
        elif (weather == "snow"):
            weather_id = 3
        elif (weather == "clouds"):
            weather_id = 4
        elif (weather == "thunderstorm"):
            weather_id = 5
        elif (weather == "mist"):
            weather_id = 6
        elif (weather == "smoke"):
            weather_id = 7
        elif (weather == "haze"):
            weather_id = 8
        elif (weather == "dust"):
            weather_id = 9
        elif (weather == "fog"):
            weather_id = 10
        elif (weather == "sand"):
            weather_id = 11
        elif (weather == "ash"):
            weather_id = 12
        elif (weather == "squalls"):
            weather_id = 13
        elif (weather == "tornado"):
            weather_id = 14

        new_temp = pytemperature.k2f(temperature)
        new_temp = int(round(new_temp))


        main_weather_tup = (current_time_stamp, new_temp, weather_id, weather)
        return main_weather_tup



    def get_print_data(self, url):
        ''' Requests data from the Mprint API along with the 
        get_weather_data function and extracts the necessary 
        information into the database'''

        r = requests.get(url, headers=self.headers)
        request_dict = json.loads(r.text) # And then load it into a dictionary

        sub_queues = request_dict["result"][0]["sub_queues"]
        building_id = request_dict["result"][0]["display_name"]
        floor = 2

        if(building_id == "Shapiro Library Rm 1000 2"):
            floor = 1

        for queue in sub_queues:
            jobs_queue = queue["jobs"]
            # goes through all the printer queues
            for job in jobs_queue:   
                # this limits the amount of inserts in a database to 20. 
                if(self.counter == 20):
                    return;        
                if job["id"] not in self.all_jobs:
                    creation_date = job["creation_time"]
                    match = re.search(r'\d{4}-\d{2}-\d{2}', creation_date)
                    date_in_list = match.group(0).split('-')

                    day = int(date_in_list[2])
                    month = int(date_in_list[1])
                    year = int(date_in_list[0])
                    

                    parsedDate = datetime.date(year,month,day)
                    currentWeekDay = parsedDate.weekday()

                    job_tuple = (job["id"], creation_date, currentWeekDay, building_id)
                    weather_tup = self.get_weather_data()

                    conn = sqlite3.connect(self.db)
                    cur = conn.cursor()

                    # inserting into temperature table
                    cur.execute(''' INSERT OR IGNORE INTO tempInstance (temp_id, temp, weather_id, print_id) 
                            VALUES (?,?,?,?);''', 
                            (str(weather_tup[0]), weather_tup[1], weather_tup[2], job_tuple[0],))

                    # inserting into weather table
                    cur.execute(''' INSERT OR IGNORE INTO weather (weather_id, weather_type) 
                            VALUES (?,?);''', 
                            (weather_tup[2], weather_tup[3],))

                    # inserting into building table
                    cur.execute(''' INSERT OR IGNORE INTO buildings (buildingId, floor) 
                            VALUES (?,?);''', 
                            (job_tuple[3], floor,))

                    conn.commit()
                    conn.close()

                    self.all_jobs.append(job_tuple)
                    self.counter += 1



    def insert_into_print_table(self):
        ''' insert data into prints table '''
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()

        for tup in self.all_jobs:

            cur.execute(''' INSERT OR IGNORE INTO prints (print_id, timeStamp, dayOfWeek, buildingId) 
                            VALUES (?,?,?,?);''', 
                            (tup[0], tup[1], tup[2], tup[3],))
        conn.commit()
        conn.close()



    def calculation_table1(self):
        ''' calculates the data and visual for avg prints/hour by temperature'''
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()

        temp = cur.execute(""" select dayOfWeek, timeStamp, temp
            from prints p left join tempInstance t on p.print_id = t.print_id
            ORDER by temp, dayOfWeek, t.temp_id ASC;""",).fetchall()

        conn.commit()
        conn.close()

        temperature = temp[0][2]
        # print(temperature)
        day = temp[0][0]
        hours = 0.0

        num_prints = 0

        first_date = 0.0
        last_date = 0.0

        master_dict = {}

        # calculates the avg. number of prints by hour
        for i in temp:
            # if the temp is equal to the current temp and its the same day
            if temperature == i[2] and day == i[0]:
                    match = re.search(r'\d{2}:\d{2}:\d{2}', i[1])
                    date_in_list = match.group(0).split(':')

                    minute = float(date_in_list[1])
                    hour = float(date_in_list[0])

                    if(hour < 1.0):
                        hour = 24.0

                    #print(hour + (minute/60))

                    # set the first date
                    if first_date == 0.0:
                        first_date = hour + (minute/60)
                    # set the last date
                    if (hour + (minute/60)) > last_date:
                        last_date = hour + (minute/60)

                    num_prints += 1
                    hours = last_date - first_date
            else:
                # print(temperature)
                if temperature in master_dict:
                    master_dict[temperature][0] += num_prints
                    master_dict[temperature][1] += hours
                else:
                    list_temp = [num_prints, hours]
                    master_dict[temperature] = list_temp

                temperature = i[2]
                day = i[0]
                first_date = 0.0
                last_date = 0.0
                num_prints = 1
                hours = 0.0

        if temperature in master_dict:
            master_dict[temperature][0] += num_prints
            master_dict[temperature][1] += hours
        else:
            list_temp = [num_prints, hours]
            master_dict[temperature] = list_temp

        y_axis = []
        x_axis = []

        # We are using 30 mintutes a an estimation threshold. 
        # if we recoreded data for less than 30 minutes, 
        # then we can't assume the number of prints
        # for example, if there were 10 prints in a second of recording, 
        # we cant assume that theres gonna be 3600 prints per hour. --> highly unrealistic.
        # however, if we recoreded for more than 30 minutes,
        # it is SAFER to assume the rate at which prints are made per hour. 
        for k, v in master_dict.items():
            if(v[1] > 0.5):
                v[0] /= v[1]
            del v[-1]
            v[0] = round(v[0])
            master_dict[k] = v[0]
            y_axis.append(v[0])
            x_axis.append(k)

        # create plotly scatter graph
        fig = px.scatter(x=x_axis, y=y_axis, trendline="ols")
        fig.update_layout(
            title="Average Number of Prints/Hour by Temperature",
            xaxis_title="Temperature in Degrees Fahrenheit",
            yaxis_title="Avg. Prints/Hour",
        )
        fig.show()

        # writes to calculations file
        with open("Calculations.txt", "w") as f:
            f.write("TEMPERATURE, AVG. NUMBER OF PRINTS PER HOUR\n")
            for k, v in master_dict.items():
                f.write(str(k) + ", " + str(v) + "\n")
            f.write("\n")
            f.write("\n")
        f.close()





    def calculation_table2(self):
        ''' calculates the data and visual for avg prints/hour by ugli floor'''
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()

        temp = cur.execute(""" select dayOfWeek, timeStamp, buildingId
            from prints p left join tempInstance t on p.print_id = t.print_id
            ORDER by buildingId, dayOfWeek, t.temp_id ASC;""",).fetchall()

        building = temp[0][2]
        # print(temperature)
        day = temp[0][0]
        hours = 0.0

        num_prints = 0

        first_date = 0.0
        last_date = 0.0

        master_dict = {}

        # calculates the avg. number of prints by hour
        for i in temp:
            # if the temp is equal to the current temp and its the same day
            if day == i[0] and building == i[2]:
                    match = re.search(r'\d{2}:\d{2}:\d{2}', i[1])
                    date_in_list = match.group(0).split(':')

                    minute = float(date_in_list[1])
                    hour = float(date_in_list[0])

                    if(hour < 1.0):
                        hour = 24.0

                    #print(hour + (minute/60))

                    # set the first date
                    if first_date == 0.0:
                        first_date = hour + (minute/60)
                    # set the last date
                    if (hour + (minute/60)) > last_date:
                        last_date = hour + (minute/60)

                    num_prints += 1
                    hours = last_date - first_date


            else:
                floor = cur.execute(""" select floor from buildings where buildingId = (?);""",(building,)).fetchone()

                # print(temperature)
                if floor in master_dict:
                    master_dict[floor][0] += num_prints
                    master_dict[floor][1] += hours
                else:
                    list_temp = [num_prints, hours]
                    master_dict[floor] = list_temp

                day = i[0]
                building = i[2]
                first_date = 0.0
                last_date = 0.0
                num_prints = 1
                hours = 0.0

        # this accounts for the last iteration of the for loop. 
        floor = cur.execute(""" select floor from buildings where buildingId = (?);""",(building,)).fetchone()

        if floor in master_dict:
            master_dict[floor][0] += num_prints
            master_dict[floor][1] += hours
        else:
            list_temp = [num_prints, hours]
            master_dict[floor] = list_temp


        y_axis = []
        x_axis = []

        # find the average number of prints by hour
        for k, v in master_dict.items():
            # We are using 30 mintutes a an estimation threshold. 
            # if we recoreded data for less than 30 minutes, 
            # then we can't assume the number of prints
            # for example, if there were 10 prints in a second of recording, 
            # we cant assume that theres gonna be 3600 prints per hour. --> highly unrealistic.
            # however, if we recoreded for more than 30 minutes,
            # it is SAFER to assume the rate at which prints are made per hour. 
            if(v[1] > 0.5):
                v[0] /= v[1]
            del v[-1]

            v[0] = round(v[0])
            master_dict[k] = v[0]
            y_axis.append(v[0])
            x_axis.append(k[0])


        conn.commit()
        conn.close()

        # writes to calculations file
        with open("Calculations.txt", "a") as f:
            f.write("UGLI FLOOR, AVG. NUMBER OF PRINTS PER HOUR\n")
            for k, v in master_dict.items():
                f.write(str(k[0]) + ", " + str(v) + "\n")
            f.write("\n")
            f.write("\n")
        f.close()

        data = [go.Bar(
            x=x_axis,
            y=y_axis,
            marker={
                'color': y_axis,
                'colorscale': px.colors.diverging.Tealrose_r
            }
        )]

        # fig = go.Figure([go.Bar(x=x_axis, y=y_axis)])
        fig = go.FigureWidget(data=data)
        fig.update_layout(
            title="Average Number of Prints/Hour by Ugli Floor",
            xaxis_title="Shapiro Undergraduate Library (Ugli) Floor",
            yaxis_title="Avg. Prints/Hour",
        )

        fig.show()




    def calculation_table3(self):
        ''' calculates the data and visual for avg prints/hour by weather condition'''
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()

        temp = cur.execute(""" select dayOfWeek, timeStamp, weather_id
            from prints p left join tempInstance t on p.print_id = t.print_id
            ORDER by weather_id, dayOfWeek, t.temp_id ASC;""",).fetchall()

        building_weather = temp[0][2]
        # print(temperature)
        day = temp[0][0]
        hours = 0.0

        num_prints = 0

        first_date = 0.0
        last_date = 0.0

        master_dict = {}

        #calculate average prints/hour
        for i in temp:
            # if the temp is equal to the current temp and its the same day
            if day == i[0] and building_weather == i[2]:
                    match = re.search(r'\d{2}:\d{2}:\d{2}', i[1])
                    date_in_list = match.group(0).split(':')

                    minute = float(date_in_list[1])
                    hour = float(date_in_list[0])

                    if(hour < 1.0):
                        hour = 24.0

                    #print(hour + (minute/60))

                    # set the first date
                    if first_date == 0.0:
                        first_date = hour + (minute/60)
                    # set the last date
                    if (hour + (minute/60)) > last_date:
                        last_date = hour + (minute/60)

                    num_prints += 1
                    hours = last_date - first_date


            else:
                weather_type = cur.execute(""" select weather_type from weather where weather_id = (?);""",(building_weather,)).fetchone()

                # print(temperature)
                if weather_type in master_dict:
                    master_dict[weather_type][0] += num_prints
                    master_dict[weather_type][1] += hours
                else:
                    list_temp = [num_prints, hours]
                    master_dict[weather_type] = list_temp

                day = i[0]
                building_weather = i[2]
                first_date = 0.0
                last_date = 0.0
                num_prints = 1
                hours = 0.0

        # this accounts for the last iteration of the for loop. 
        weather_type = cur.execute(""" select weather_type from weather where weather_id = (?);""",(building_weather,)).fetchone()

        if weather_type in master_dict:
            master_dict[weather_type][0] += num_prints
            master_dict[weather_type][1] += hours
        else:
            list_temp = [num_prints, hours]
            master_dict[weather_type] = list_temp

        y_axis = []
        x_axis = []

        # find the average number of prints by hour
        for k, v in master_dict.items():
            # We are using 30 mintutes a an estimation threshold. 
            # if we recoreded data for less than 30 minutes, 
            # then we can't assume the number of prints
            # for example, if there were 10 prints in a second of recording, 
            # we cant assume that theres gonna be 3600 prints per hour. --> highly unrealistic.
            # however, if we recoreded for more than 30 minutes,
            # it is SAFER to assume the rate at which prints are made per hour. 
            if(v[1] > 0.5):
                v[0] /= v[1]
            del v[-1]

            v[0] = round(v[0])
            master_dict[k] = v[0]
            y_axis.append(v[0])
            x_axis.append(k[0])

        conn.commit()
        conn.close()

        with open("Calculations.txt", "a") as f:
            f.write("WEATHER CONDITION, AVG. NUMBER OF PRINTS PER HOUR\n")
            for k, v in master_dict.items():
                f.write(str(k[0]) + ", " + str(v) + "\n")
        f.close()

        y_axis.sort()
        data = [go.Bar(
            x=x_axis,
            y=y_axis,
            marker={
                'color': y_axis,
                'colorscale': 'Magma'
            }
        )]

        fig = go.FigureWidget(data=data)


        # fig = go.Figure([go.Bar(x=x_axis, y=y_axis)])
        fig.update_layout(
            title="Average Number of Prints/Hour by Weather Condition",
            xaxis_title="Weather Condition",
            yaxis_title="Avg. Prints/Hour",
        )
        fig.show()



    

# This is our schema

# CREATE TABLE buildings(
#     buildingId TEXT PRIMARY KEY,
#     floor INTEGER NOT NULL
# );

# CREATE TABLE prints(
#     print_id TEXT PRIMARY KEY,
#     timeStamp TEXT,
#     dayOfWeek INTEGER NOT NULL,
#     buildingId TEXT NOT NULL,
#     FOREIGN KEY (buildingId) REFERENCES buildings(buildingId)
# );

# CREATE TABLE weather(
#     weather_id INTEGER PRIMARY KEY,
#     weather_type TEXT NOT NULL
# );

# CREATE TABLE tempInstance(
#     temp_id TEXT PRIMARY KEY,
#     temp INTEGER NOT NULL,
#     weather_id INTEGER NOT NULL,
#     print_id TEXT NOT NULL UNIQUE,
#     FOREIGN KEY (weather_id) REFERENCES weather(weather_id),
#     FOREIGN KEY (print_id) REFERENCES prints(print_id)
# );

# CREATE TABLE temp_results(
#     temp INTEGER PRIMARY KEY,
#     num_prints INTEGER,
#     num_days_involved INTEGER
# );





def main():
    iteration = 0
    run = API_ACCESS()

    # calls the api and inserts in database for one minute. 
    while iteration < 10:
        print("ITERATION: ")
        print(iteration)
        print("\n")
        
        # this accounts fo the fact that the 20 insert limit can be 
        # biased to the first api call. 
        if (iteration % 2 == 0):
            run.get_print_data(run.url1)
            run.get_print_data(run.url2)
        else:
            run.get_print_data(run.url2)
            run.get_print_data(run.url1)

        run.insert_into_print_table()
        time.sleep(6)
        iteration += 1

    print("CALCULATION 1:")
    run.calculation_table1()
    print("CALCULATION 2:")
    run.calculation_table2()
    print("CALCULATION 3:")
    run.calculation_table3()


if __name__ == "__main__":
    main()
