import psycopg2
import config
import pandas as pd

config_ = config.Config()

def connect_to_database():
    print("Connecting to database")
    conn = psycopg2.connect("dbname="+config_.db_name+" user="+config_.db_user+" password="+config_.db_password+" host=localhost")
    cur = conn.cursor()
    return cur, conn


def get_distinct_records(cur):
    cur.execute("SELECT DISTINCT business_id, name, address, latitude, longitude, city, postal_code, state"
                " FROM restaurants ORDER BY business_id, name, address, latitude, longitude, city, postal_code, state")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['business_id', 'name', 'address', 'latitude', 'longitude', 'city', 'postal_code', 'state'])
    df.to_csv(config_.path+"\\restaurants_distinct.csv", index=False)


def send_query_for_quality_check(cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['business_id', 'name', 'address', 'latitude', 'longitude', 'city', 'postal_code', 'state', 'geom'])
    return df


def low_quality_data(cur):

    query1 = "select * from dist_rest where length(postal_code)<>5"
    query1_result = send_query_for_quality_check(cur, query1)

    query2 = "select * from dist_rest where postal_code not like '94%'"
    query2_result = send_query_for_quality_check(cur, query2)

    query3 = "select * from dist_rest where postal_code is null"
    query3_result = send_query_for_quality_check(cur, query3)

    query4 = "SELECT * FROM dist_rest WHERE postal_code ~ '[^0-9]'"
    query4_result = send_query_for_quality_check(cur, query4)

    query5 = "select * from dist_rest where latitude is null or longitude is null"
    query5_result = send_query_for_quality_check(cur, query5)

    query6 = "select * from dist_rest where upper(address) like '%OFF THE GRID%' or upper(address) like " \
             "'%OTG%' or upper(address) like '%APPROVED LOCATION%'"
    query6_result = send_query_for_quality_check(cur, query6)

    query7 = "select * from dist_rest where address ~ '^[0-9 ]*$'"
    query7_result = send_query_for_quality_check(cur, query7)

    df = query1_result.append(query2_result)
    df = df.append(query3_result)
    df = df.append(query4_result)
    df = df.append(query5_result)
    df = df.append(query6_result)
    df = df.append(query7_result)

    df = df.drop_duplicates(subset=['business_id'], keep='first')
    number_of_low_quality = len(df)
    print("Number of low quality data is: "+str(number_of_low_quality))

    query8 = "select * from dist_rest where name=upper(name) or name=lower(name)"
    query8_result = send_query_for_quality_check(cur, query8)

    query9 = "select * from dist_rest where name like '%@%'"
    query9_result = send_query_for_quality_check(cur, query9)

    query10 = "select * from dist_rest where address ~ '^[^0-9]+$'"
    query10_result = send_query_for_quality_check(cur, query10)

    query11 = "select * from dist_rest where address=upper(address)"
    query11_result = send_query_for_quality_check(cur, query11)

    df = df.append(query8_result)
    df = df.append(query9_result)
    df = df.append(query10_result)
    df = df.append(query11_result)

    df = df.drop_duplicates(subset=['business_id'], keep='first')
    number_of_medium_quality = len(df) - number_of_low_quality
    print("Number of medium quality data is: " + str(number_of_medium_quality))

cur, con = connect_to_database()
low_quality_data(cur)
#get_distinct_records(cur)

