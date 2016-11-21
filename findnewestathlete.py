##################################################
# Anton Rubisov                         20150119 #
# University of Toronto Sports Analytics Group   #
#                                                #
# Find newest registered Crossfit athlete by     #
# jumping through athleteID numbers and checking #
# whether that page exists. This accounts for    #
# occasional gaps in the athlete numbers, where  #
# accounts may have been made but later deleted. #
##################################################

# Required headers
import urllib2                              # Read webpages
import re                                   # Regex, removing characters
from BeautifulSoup import BeautifulSoup     # Beautiful Soup!
import time                                 # TIME WHAT IS TIME

#reinvestigate names: 230508, 230985, 231044

def main():

    athlete_id = 407630
    newest = 402606
    max_id = 500000
    jump_back = 0
    cons_not_found = 0
    start_time = time.time()

    while athlete_id < max_id:

        if athlete_id % 10 == 0:
            print "At athlete_id={}. Newest={}".format(athlete_id, newest)
            print "--- {} seconds ---".format(round(time.time() - start_time))

        athl_url = "http://games.crossfit.com/athlete/{}".format(athlete_id)
        athl_page = urllib2.urlopen(athl_url)
        soup = BeautifulSoup(athl_page)

        soup_name_str = soup.find('title').string

        try:
            athl_name = re.search('Athlete: (.*?) \| CrossFit Games', soup_name_str)
            name = str(athl_name.group(1))
        except UnicodeEncodeError:
            print "At athlete_id={}. Newest={}".format(athlete_id, newest)
            print "Athlete Name", athl_name

        if name == 'Not found':
            if cons_not_found == 100:
                if jump_back == 9:
                    print "Newest found so far: {}".format(newest)
                    athlete_id = max_id
                    break
                athlete_id -= 200
                cons_not_found = 0
                jump_back += 1
            athlete_id += 1
            cons_not_found += 1
            continue
        else:
            newest = athlete_id
            cons_not_found = 0
            athlete_id += 5000

    print "Newest Athlete ID: {}".format(newest)

if __name__ == "__main__":
    main()