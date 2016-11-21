##################################################
# Anton Rubisov                         20151209 #
# University of Toronto Sports Analytics Group   #
#                                                #
# Scrape the list of athleteIDs for athletes     #
# that competed in the 2015 Open. The Mongo      #
# collection crossfitSoup contains athletes up   #
# to the 2014 open, and this will give us a set  #
# of data against which we can do hypothesis     #
# testing.                                       #
##################################################

# Required headers
import urllib2                              # Read webpages
import re                                   # Regex, removing characters
from BeautifulSoup import BeautifulSoup     # Beautiful Soup, v. 3.2.1
import time                                 # TIME WHAT IS TIME
from pymongo import MongoClient             # MongoDB for Python, v. 2.7.2

host = 'localhost'
database = 'crossfitSoup15'
collection = 'athleteId'


def mongo_connection():

    client = MongoClient()
    db = client[database]
    col = db[collection]
    return col


def main():

    col = mongo_connection()

    start_time = time.time()

    for page in range(1, 1533):

        if page % 10 == 0:
            print "Runtime {} seconds --- Scraping page {}.".format(
                round(time.time() - start_time), page)

        athl_url = ("http://games.crossfit.com/scores/leaderboard.php?stage=5&"
        "sort=0&division=1&region=0&numberperpage=100&page={}&"
        "competition=0&year=15").format(page)

        athl_page = urllib2.urlopen(athl_url)
        soup = BeautifulSoup(athl_page)

        for athlete in soup.findAll('td','name'):
            athlete_id = int(re.search('http://games.crossfit.com/athlete/(.*)', athlete.a['href']).group(1))
            athlete = {"gender": 1, "id": athlete_id}
            col.insert(athlete)

if __name__ == "__main__":
    main()