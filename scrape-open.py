##################################################
# Anton Rubisov                         20150109 #
# University of Toronto Sports Analytics Group   #
#                                                #
# Scraping CrossFit Open information from        #
# http://games.crossfit.com/athlete/             #
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
database = 'crossfitSoup15'
collection = 'athleteRankings'
collectionID = 'athleteId'

COMPS = {'open': 0, 'regionals': 1, 'games': 2}

REGIONS = {'Worldwide': 0, 'Africa': 1, 'Asia': 2, 'Australia': 3,
    'Canada East': 4, 'Canada West': 5, 'Central East': 6, 'Europe': 7,
    'Latin America': 8, 'Mid Atlantic': 9, 'North Central': 10,
    'North East': 11, 'Northern California': 12, 'North West': 13,
    'South Central': 14, 'South East': 15, 'Southern California': 16,
    'South West': 17}


def mongo_connection():
    client = pymongo.MongoClient()
    db = client[database]
    return db


def conv_nstr_to_n(input_str):
    if input_str == '--':
        return -1
    return int(input_str)


def conv_tstr_to_s(input_str):
    if input_str == '--':
        return -1
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


def main():

    try:
        client = pymongo.MongoClient()
        db = client[database]
        col = db[collection]
        colID = db[collectionID]

        pool = HTTPConnectionPool('games.crossfit.com', maxsize=1)

        start_time = time.time()

        max_id = col.find().sort("athlete_id", pymongo.DESCENDING)[0]["athlete_id"]
        #print max_id

        sorted_id = colID.find({"id": {"$gt": max_id}}).sort("id", pymongo.ASCENDING)
        #sorted_id = colID.find().limit(10)

        for i, athlete_id in enumerate(sorted_id):
            #for athlete_id in [109651]:
            athlete_id = athlete_id['id']

            athl_url = "/athlete/{}".format(athlete_id)
            athl_page = pool.request('GET', athl_url, preload_content=False)
            soup = BeautifulSoup(athl_page)

            athl_stat = {'athlete_id': athlete_id}

            soup_name_str = soup.find('title').string
            athl_name = re.search('Athlete: (.*?) \| CrossFit Games', soup_name_str)
            athl_stat['athlete_name'] = athl_name.group(1).encode('utf-8')

            if athl_stat['athlete_name'] == 'Not found':
                continue

            soup_profile_details = soup.find('div',
                attrs={'class': 'profile-details'}).findAllNext('dt')

            for item in soup_profile_details:
                item_name = str(re.search('(.*?):', item.string).group(1))
                athl_stat[item_name] = item.findNext('dd').string
                if athl_stat[item_name] is None:
                    athl_stat[item_name] = item.findNext('a').string
                athl_stat[item_name] = str(athl_stat[item_name])

            soup_profile_stats = soup.find('div',
                attrs={'class': 'profile-stats'}).findAllNext('td')
            profile_stats_strings = [str(item.string) for item in soup_profile_stats]
            profile_stats = dict(itertools.izip_longest(*[iter(profile_stats_strings)] * 2, fillvalue=""))
            athl_stat.update(profile_stats)

    ### Begin cleanup #############################################################

            for item in ['Max Pull-ups', 'Fight Gone Bad', 'Age']:
                athl_stat[item] = conv_nstr_to_n(athl_stat[item])

            for item in ['Filthy 50', 'Run 5k', 'Sprint 400m',
                            'Fran', 'Helen', 'Grace']:
                athl_stat[item] = conv_tstr_to_s(athl_stat[item])

            for item in ['Deadlift', 'Clean &amp; Jerk', 'Snatch', 'Back Squat',
                            'Weight']:
                athl_stat[item] = conv_wstr_to_lbs(athl_stat[item])

            athl_stat['Height'] = conv_height_to_in(athl_stat['Height'])
            athl_stat['Gender'] = conv_gender_to_n(athl_stat['Gender'])
            athl_stat['Region'] = conv_region_to_n(athl_stat['Region'])

    ## End Cleanup ################################################################

            for comp_year in ['15']:
                for comp in {'open': 0}:
                    region = 0
                    if comp == 'regionals':
                        region = athl_stat['Region']

                    results_url = (
                        "/scores/leaderboard.php?"
                        "competition={}&stage=5&division={}&region={}&"
                        "numberperpage=1&userid={}&year={}&showtoggles=1&"
                        "hidedropdowns=1").format(COMPS[comp], athl_stat['Gender'],
                         region, athlete_id, comp_year)

                    results_page = pool.request('GET', results_url, preload_content=False)
                    soup = BeautifulSoup(results_page)

                    name = soup.find('td', attrs={'class': 'name'})
                    if name is None:
                        continue
                    if name.a.string is None:
                        continue
                    if name.a.string.encode('utf-8') != athl_stat['athlete_name']:
                        continue

                    rank = soup.find('td', attrs={'class': 'number'})
                    if rank is None:
                        continue

                    result = rank.string
                    result = result.split(' ')
                    overall_rank = conv_nstr_to_n(result[0])
                    overall_score = result[1].replace('(','')
                    overall_score = overall_score.replace(')','')
                    overall_score = conv_nstr_to_n(overall_score)

                    athl_stat['20{}-{}-overall-rank'.format(comp_year, comp)] = overall_rank
                    athl_stat['20{}-{}-overall-score'.format(comp_year, comp)] = overall_score

                    for item in rank.findAllNext('td', attrs={'class': 'score-cell '}):
                        workout = re.findall(r'\d+',
                            item.findAllNext('span', limit=2)[1]['class'])

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
                        athl_stat['20{}-{}-workout{}-rank'.format(comp_year, comp, workout[0])] = \
                            result_rank
                        athl_stat['20{}-{}-workout{}-score'.format(comp_year, comp, workout[0])] = \
                            result_score

            ### DISPLAY ONLY
            #print "Time per athlete: {}s".format(round)
            #print "First URL request: {}s. First soup: {}s. Athl Deets: {}s. Cleanup: {}s All scores: {}s".format(
                #round(t2-t1,4),round(t3-t2,4),round(t4-t3,4),round(t5-t4,4),round(t6-t5,4))


            col.insert(athl_stat)
            #pprint(athl_stat)

            if i % 100 == 0:
                remaining = int(colID.find({"id": {"$gt": athlete_id}}).count())
                runtime = time.time() - start_time
                time_remaining = remaining * runtime / (i + 1)
                print ("Runtime {} seconds. Scraped {}th athlete, ID {}. "
                "Est Time Remaining: {} hrs. Scrape Rate: {}s/athlete.").format(
                    round(runtime), i, athlete_id,
                    round(time_remaining / 3600, 2),
                    round(runtime/(i+1),2))
    finally:
        print "Bailed on athlete ID {}".format(athlete_id)

if __name__ == "__main__":
    main()