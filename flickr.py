import requests
import pandas as pd
import mysql.connector
from datetime import datetime

pd.set_option('display.max_colwidth', 255)

# helper methods
def get_images_dictionary(keyword:str, size:str) -> dict:
    params = {
        'api_key' : 'b6ed4e7688890936b0aefad90d9dfc30',
        'secret' : '01f10c4a169d4062',
        'tags' : keyword,
        'method': 'flickr.photos.search',
        'format' : 'json',
        'nojsoncallback': '1',
        'per_page' : size
    }
    url = "https://api.flickr.com/services/rest/"
    res = requests.get(url, params=params)
    photos_dict = res.json()
    return photos_dict
    
def get_image_url(server_id, photo_id, secret):
    image_url = 'https://live.staticflickr.com/'+server_id+'/'+photo_id+'_'+secret+'_w.jpg'
    return image_url

def insert_rows_to_mysql_db(values_list_to_insert:list):
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="my-secret-pw",
        database="flickr"
    )
    mycursor = mysql_connection.cursor()
    sql_query = "INSERT INTO images (imageURL, scrapeTime, keyword) VALUES (%s, %s, %s)"
    inserted_successfully = True
    for fields_row in values_list_to_insert:
        try:
            mycursor.execute(sql_query,fields_row)
            mysql_connection.commit()
        except:
            inserted_successfully = False
            print('Oops, image: ', fields_row, ' already exist on the DB')
    if inserted_successfully == True:
        print('inserted ', size, ' rows successfully :)')
    mysql_connection.close()


# required functions
def scrape(keyword:str, size:int):
    photos_dict = get_images_dictionary(keyword,str(size))

    photos_list = photos_dict['photos']['photo']
    for photo in photos_list:
        photo['image_url'] = get_image_url(server_id=photo['server'], photo_id=photo['id'] , secret=photo['secret'])

    values_list_to_insert = []
    for photo in photos_list:
        image_url = photo['image_url']
        time_stamp = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        values_list_to_insert.append((image_url,time_stamp,keyword))
        
    insert_rows_to_mysql_db(values_list_to_insert)

def search(minScrapeTime, maxScrapeTime, keyword, size) -> pd.DataFrame:
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="my-secret-pw",
        database="flickr"
    )
    sql_query = """SELECT * 
                FROM images 
                WHERE keyword = '{keyword}' 
                        and scrapeTime between '{minScrapeTime}' and '{maxScrapeTime}' 
                        limit {size}""".format(keyword=keyword, minScrapeTime=minScrapeTime, 
                                               maxScrapeTime=maxScrapeTime, size=size)
    df = pd.read_sql(sql_query,mysql_connection)
    return df

# ################# main #################
if __name__ == "__main__":
    import sys
    operation = sys.argv[1]
    
    if operation == "scrape":
        keyword = sys.argv[2]
        size = int(sys.argv[3])
        scrape(keyword, size)
    elif operation == "search":
        min_timestamp = sys.argv[2]
        max_timestamp = sys.argv[3]
        keyword = sys.argv[4]
        size = int(sys.argv[5])
        result = search(min_timestamp, max_timestamp, keyword, size)
        print(result)
    else:
        print('Oops, something is wrong. Please enter a valid function name - scrape or search')