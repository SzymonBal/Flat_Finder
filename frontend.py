from pyspark.sql import SparkSession
import streamlit as st
from geopy.geocoders import Nominatim
import requests
import json
from pyspark.sql.functions import monotonically_increasing_id
import math
import time
from streamlit_folium import folium_static
import folium
from pyspark.sql.functions import split , collect_list, size
import pandas as pd

account = st.query_params.get('account_type')

if st.query_params.get("is_logged_in") != "True":
    st.error("Musisz się zalogować, aby zobaczyć hipopotama.")

else:
    # Inicjalizacja sesji Spark
    spark = SparkSession.builder \
        .appName("Reading Data from PostgreSQL") \
        .config("spark.driver.extraClassPath", "postgresql-42.7.2.jar") \
        .config("spark.executor.extraClassPath", "postgresql-42.7.2.jar") \
        .getOrCreate()
    
    # Wczytanie danych z bazy danych PostgreSQL
    def load_data():
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://my_database:5432/my_database") \
            .option("dbtable", "flatsRZ") \
            .option("user", "my_user") \
            .option("password", "123") \
            .load()
        return df


    def get_latlon(address):
        geolocator = Nominatim(user_agent="asd")
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None



    # Funkcja do filtrowania danych w Sparku na podstawie wybranego zakresu liczby pokoi
    def filter_data(df, min_rooms, max_rooms, region):
        filtered_df = df.filter((df["Liczba pokoi"] >= min_rooms) & (df["Liczba pokoi"] <= max_rooms))
        filtered_df = filtered_df.filter((df['region'] == region))
        return filtered_df


    def filter_distance(final_df, dystans):
        final = final_df.filter((final_df["Dystans"] <= dystans))
        return final

    def filter_distance2(final_df, dystans):
        final = final_df.filter((final_df["Dystans 2"] <= dystans))
        return final



    def control_distance(lat, lon, lat2, lon2):
        lat1, lon1, lat2, lon2 = map(math.radians, [lat, lon, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        R = 6371.0
        controled_distance = R * c
        return controled_distance

    def distance(lat, lon, latlon_value, dystans, percent):

        api_url = "https://api.tomtom.com/routing/1/calculateRoute/"
        api_key = ''
        distance_values = []
        i = 0
        percent_cell = st.empty()
        for coor in latlon_value:
            i = i + 1
            percent_show = i/percent * 100
            percent_show = round(percent_show,0)
            percent_cell.text(f"Aktualny postęp: {percent_show}%")
            coords = coor.split(',')
            coords_lat = float(coords[0])
            coords_lon = float(coords[1])
            c_cist = control_distance(lat, lon, coords_lat, coords_lon)
            if c_cist <= dystans:
                tomtom_url = f"{api_url}{lat},{lon}:{coor}/json?key={api_key}"
                try:
                    
                    response = requests.get(tomtom_url)
                    response.raise_for_status()  
                    json_response = response.json()
                    
                    distance = json_response['routes'][0]['summary']['lengthInMeters']
                    distance_value = distance / 1000
                    distance_values.append(distance_value)
                    
                    print("Distance to destination is:", distance_value)
                except requests.exceptions.RequestException as e:
                    print("Error making API request:", e)
                except KeyError:
                    print("Error parsing API response. Check the response format.")
                except json.JSONDecodeError:
                    print("Error decoding JSON. Check the response content.")
                
            else:
                distance_values.append(1000.0)
        #st.write(distance_values)
        return distance_values  


        
    # Interfejs użytkownika w Streamlit
    def main():
        st.title('Spark + Streamlit Integration')
        st.write("Typ konta:", account)
        # Wczytanie danych
        df = load_data()

        # Pobranie minimalnej i maksymalnej wartości liczby pokoi
        min_num_rooms = int(df.select("Liczba pokoi").agg({"Liczba pokoi": "min"}).collect()[0][0])
        max_num_rooms = int(df.select("Liczba pokoi").agg({"Liczba pokoi": "max"}).collect()[0][0])

        # Element interfejsu użytkownika do wyboru minimalnej liczby pokoi
        min_rooms = st.slider("Wybierz minimalną liczbę pokoi:", min_value=min_num_rooms, max_value=max_num_rooms, value=min_num_rooms)

        # Element interfejsu użytkownika do wyboru maksymalnej liczby pokoi
        max_rooms = st.slider("Wybierz maksymalną liczbę pokoi:", min_value=min_num_rooms, max_value=max_num_rooms, value=max_num_rooms)


        region = st.text_input("Miasto:")


        street= st.text_input("ulica 1:")
        number = st.text_input("Ulica 2:")
        
        street2 = None
        number2 = None

        if account == 'premium':
            street2 = st.text_input("Numer bloku 1:")
            number2 = st.text_input("Numeb bloku 2:")
        
        address = region + " " + street + " " + number
        if street2 and number2:
            region2 = region    
            address2 = region2 + " " + street2 + " " + number2
        else:
            address2 = None

        # Pole tekstowe do wprowadzenia adresu
        dystans = st.selectbox("Podaj dystans:", [1, 2, 3, 4, 5,10, 500])

        # Przetworzenie danych w Sparku na podstawie wybranego zakresu liczby pokoi
        filtered_data = filter_data(df, min_rooms, max_rooms, region)


        latlon_value = filtered_data.select("latlon").rdd.flatMap(lambda x: x).collect()
        
        # Wyświetlenie wartości latlon_value
        st.write("Wartość latlon_value:", latlon_value)

        # Dodaie kolumny z indeksem do istniejącego DataFrame
        filtered_data_with_index = filtered_data.withColumn("index", monotonically_increasing_id())

        if st.button("Pobierz współrzędne") and address:
            lat, lon = get_latlon(address)
            percent = filtered_data.count()
            lat2, lon2 = None, None
            if address2:
                lat2, lon2 = get_latlon(address2)
            else:
                st.write('Wybrałeś 1 Adres')
            if lat is not None and lon is not None:
                dist = distance(lat, lon, latlon_value, dystans, percent)
                if lon2 is not None and lat2 is not None:
                    dist2 = distance(lat2, lon2, latlon_value, dystans, percent)
                #st.success(f"Szerokość: {lat}, Długość: {lon}, Dystans: {dist} km")
                
                # Utworznie DataFrame z listy `dist`
                dist_df = spark.createDataFrame([(i, dist[i]) for i in range(len(dist))], ["index", "Dystans"])           
                final_df = filtered_data_with_index.join(dist_df, on="index").drop("index")

                final = filter_distance(final_df,dystans)
                if address2:
                    final_df = final_df.withColumn("index", monotonically_increasing_id())
                    dist_df2 = spark.createDataFrame([(i, dist2[i]) for i in range(len(dist2))], ["index", "Dystans 2"])
                    final_df = final_df.join(dist_df2, on="index").drop("index")

                    final = filter_distance(final_df,dystans)
                    final2 = filter_distance2(final,dystans)
                    #st.write(dist_df2)
                    #st.write(dist_df)

                    mymap = folium.Map(location=[lat, lon], zoom_start=6)
                    st.write(final2.toPandas())

                    final2 = final2.withColumn('latitude', split('latlon', ',').getItem(0).cast('float'))
                    final2 = final2.withColumn('longitude', split('latlon', ',').getItem(1).cast('float'))

                    final_grouped = final2.groupBy('latitude', 'longitude').agg(collect_list('URL').alias('URLs'))
                    #st.write(final_grouped.toPandas())
                    # Dodawanie punktów na mapie
                    for row in final_grouped.collect():
                        coordinates = (row['latitude'], row['longitude'])
                        titles = '\n\n'.join(['<a href="{0}" target="_blank">{0}</a>'.format(url) for url in row['URLs']])
                        folium.Marker(coordinates, popup=titles).add_to(mymap)

                    # Wyświetlanie mapy
                    folium_static(mymap)
                else:
                    mymap = folium.Map(location=[lat, lon], zoom_start=10)
                    st.write(final.toPandas())

                    final = final.withColumn('latitude', split('latlon', ',').getItem(0).cast('float'))
                    final = final.withColumn('longitude', split('latlon', ',').getItem(1).cast('float'))

                    final_grouped = final.groupBy('latitude', 'longitude').agg(collect_list('URL').alias('URLs'))
                    #st.write(final_grouped.toPandas())
                    # Dodawanie punktów na mapie
                    for row in final_grouped.collect():
                        coordinates = (row['latitude'], row['longitude'])
                        titles = '\n\n'.join(['<a href="{0}" target="_blank">{0}</a>'.format(url) for url in row['URLs']])
                        folium.Marker(coordinates, popup=titles).add_to(mymap)

                    # Wyświetlanie mapy
                    folium_static(mymap)

            else:
                st.error("Nie udało się uzyskać współrzędnych dla podanego adresu.")

    if __name__ == "__main__":
        main()
