##################################################
# Anton Rubisov                         20150216 #
# University of Toronto Sports Analytics Group   #
#                                                #
# Iterate through the Regionals leaderboard and  #
# store workout results for all the athletes     #
# that we already have in the crossfitSoup db.   #
##################################################

# Required headers
from urllib3 import HTTPConnectionPool      # Read webpages
import itertools                            # To zip lists into a dictionary
import re                                   # Regex, removing characters
import HTMLParser                           # Parsing HTML characters
from BeautifulSoup import BeautifulSoup     # Beautiful Soup!
import pymongo                              # MongoDB for Python, v 2.7.2
from pprint import pprint                   # For printing nicely
import time                                 # TIME WHAT IS TIME

host = 'localhost'
database = 'crossfitSoup'
collection = 'athleteRankings'
collectionID = 'athleteId'

YEARS = ['12', '13', '14']

COMPS = {'regionals': 1}
    # 'open': 0,
    # 'games': 2

REGIONS = {'Africa': 1, 'Asia': 2, 'Australia': 3,
    'Canada East': 4, 'Canada West': 5, 'Central East': 6, 'Europe': 7,
    'Latin America': 8, 'Mid Atlantic': 9, 'North Central': 10,
    'North East': 11, 'Northern California': 12, 'North West': 13,
    'South Central': 14, 'South East': 15, 'Southern California': 16,
    'South West': 17}
    # 'Worldwide': 0,

GENDERS = {'indiv-male': 101, 'indiv-female': 201}


def mongo_connection():
    client = pymongo.MongoClient()
    db = client[database]
    return db


def conv_nstr_to_n(input_str):
    if input_str in ['--', '---', 'WD', 'CUT', 'DNF', 'MED', '']:
        return -1
    if input_str[-1] == 'T':
        return int(input_str[0:-1])
    return int(float(input_str))


def conv_tstr_to_s(input_str):
    if input_str == '--':
        return -1
    if "." in input_str:
        index = input_str.index(".", 0, len(input_str))
        input_str = input_str[0:index]
    l = input_str.split(':')
    return int(l[0]) * 60 + int(l[1])


def conv_wstr_to_lbs(input_str):
    if input_str == '--':
        return -1
    l = input_str.split(' ')
    if l[1] == 'lb':
        return int(l[0])
    else:
        return int(round(int(l[0]) * 2.20462))


def conv_height_to_in(input_str):
    if input_str == '--':
        return -1
    h = HTMLParser.HTMLParser()
    input_str = h.unescape(input_str)
    l = input_str.split(' ')
    if len(l) > 1:
        return int(round(int(l[0]) * 0.393701))
    else:
        input_str = input_str.replace('"', '')
        l = input_str.split('\'')
        return int(l[0]) * 12 + int(l[1])


def conv_score_to_n(input_str):
    if ":" in input_str:
        return conv_tstr_to_s(input_str)
    else:
        return conv_nstr_to_n(input_str)


def conv_gender_to_n(input_str):
    if input_str == 'Female':
        return 2
    elif input_str == 'Male':
        return 1
    else:
        return -1


def conv_region_to_n(input_str):
    if input_str in REGIONS:
        return REGIONS[input_str]
    else:
        return 0


def scrape_regionals_athlete(col, athlete, year, comp, region):
    athlete_id = int(re.search('http://games.crossfit.com/athlete/(.*)',
                                athlete.a['href']).group(1))

    found = col.find({"athlete_id": athlete_id}).count()

    if found == 0:
        return

    if year == '12':
        num_events = 6
    else:
        num_events = 7

    athl_stat = {}

    item = athlete

    try:
        for workout in range(1, num_events + 1):
            item = item.findNext('td', attrs={'class': 'score-cell '})

            result = item.next.next.next.string
            result = re.search('(.*?)\\n', result).group(1)
            result = result.split(' ')
            result_rank = conv_nstr_to_n(result[0])
            if len(result) > 1:
                result_score = str(result[1])
                result_score = result_score.replace('(', '')
                result_score = result_score.replace(')', '')
                result_score = conv_score_to_n(result_score)
            else:
                result_score = -1

            str_rank = '20{}-{}-workout{}-rank'.format(year, comp, workout)
            str_score = '20{}-{}-workout{}-score'.format(year, comp, workout)
            athl_stat[str_rank] = result_rank
            athl_stat[str_score] = result_score
    except AttributeError:
        print "AttributeError: athlete {}, workout {}".format(athlete_id, workout)
        raise AttributeError
    col.update({"athlete_id": athlete_id}, {"$set": athl_stat}, upsert=False)

### DISPLAY ONLY
#print "Time per athlete: {}s".format(round)
#print "First URL request: {}s. First soup: {}s. Athl Deets: {}s. Cleanup: {}s All scores: {}s".format(
#round(t2-t1,4),round(t3-t2,4),round(t4-t3,4),round(t5-t4,4),round(t6-t5,4))





def print_time_update(year, comp, region, gender, start_time):
    runtime = time.time() - start_time
    print ("Runtime {}s. Finished {} / {} / {} / {}").format(runtime, year,
                                                        comp, region, gender)


def main():

    try:
        client = pymongo.MongoClient()
        db = client[database]
        col = db[collection]

        pool = HTTPConnectionPool('games.crossfit.com', maxsize=1)

        start_time = time.time()

        for year in YEARS:
            for comp in COMPS:
                for region in REGIONS:
                    for gender in GENDERS:
                        results_url = (
                            "/scores/leaderboard.php?"
                            "competition={}&stage=0&division={}&region={}&"
                            "numberperpage=100&year={}&showtoggles=1&"
                            "hidedropdowns=1").format(COMPS[comp],
                             GENDERS[gender], REGIONS[region], year)

                        results_page = pool.request('GET', results_url,
                                                     preload_content=False)
                        soup = BeautifulSoup(results_page)

                        for athlete in soup.findAll('td', 'name'):
                            scrape_regionals_athlete(col, athlete, year, comp,
                                                        region)

                        print_time_update(year, comp, region, gender, start_time)
    finally:
        print results_url


if __name__ == "__main__":
    main()