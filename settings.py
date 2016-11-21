#!/usr/bin/env python3

# Required headers
import pandas as pd
import pymongo                               # MongoDB for Python3

# GLOBAL VALUES

FIELDS = {
    1 : "athlete_id",
    2 : "athlete_name" ,
    3 : "Gender" ,
    4 : "Age" ,
    5 : "Weight" ,
    6 : "Region" ,
    7 : "Deadlift" ,
    8 : "Back Squat" ,
    9 : "Clean &amp; Jerk" ,
    10 : "Snatch" ,
    11 : "Max Pull-ups" ,
    12 : "Fran" ,
    13 : "Grace" ,
    14 : "Height" ,
    15 : "Helen" ,
    16 : "Fight Gone Bad" ,
    17 : "Filthy 50" ,
    18 : "Sprint 400m" ,
    19 : "Run 5k" ,
    20 :  "2012-regionals-workout1-rank" ,
    21 : "2012-regionals-workout1-score" ,
    22 : "2012-regionals-workout2-rank" ,
    23 : "2012-regionals-workout2-score" ,
    24 : "2012-regionals-workout3-rank" ,
    25 : "2012-regionals-workout3-score" ,
    26 : "2012-regionals-workout4-rank" ,
    27 : "2012-regionals-workout4-score" ,
    28 : "2012-regionals-workout5-rank" ,
    29 : "2012-regionals-workout5-score" ,
    30 : "2012-regionals-workout6-rank" ,
    31 : "2012-regionals-workout6-score" ,
    32 : "2013-open-overall-rank" ,
    33 : "2013-open-overall-score" ,
    34 : "2013-open-workout1-rank" ,
    35 : "2013-open-workout1-score" ,
    36 : "2013-open-workout2-rank" ,
    37 : "2013-open-workout2-score" ,
    38 : "2013-open-workout3-rank" ,
    39 : "2013-open-workout3-score" ,
    40 : "2013-open-workout4-rank" ,
    41 : "2013-open-workout4-score" ,
    42 : "2013-open-workout5-rank" ,
    43 : "2013-open-workout5-score" ,
    44 : "2013-regionals-workout1-rank" ,
    45 : "2013-regionals-workout1-score" ,
    46 : "2013-regionals-workout2-rank" ,
    47 : "2013-regionals-workout2-score" ,
    48 : "2013-regionals-workout3-rank" ,
    49 : "2013-regionals-workout3-score" ,
    50 : "2013-regionals-workout4-rank" ,
    51 : "2013-regionals-workout4-score" ,
    52 : "2013-regionals-workout5-rank" ,
    53 : "2013-regionals-workout5-score" ,
    54 : "2013-regionals-workout6-rank" ,
    55 : "2013-regionals-workout6-score" ,
    56 : "2013-regionals-workout7-rank" ,
    57 : "2013-regionals-workout7-score" ,
    58 : "2014-open-overall-rank" ,
    59 : "2014-open-overall-score" ,
    60 : "2014-open-workout1-rank" ,
    61 : "2014-open-workout1-score" ,
    62 : "2014-open-workout2-rank" ,
    63 : "2014-open-workout2-score" ,
    64 : "2014-open-workout3-rank" ,
    65 : "2014-open-workout3-score" ,
    66 : "2014-open-workout4-rank" ,
    67 : "2014-open-workout4-score" ,
    68 : "2014-open-workout5-rank" ,
    69 : "2014-open-workout5-score" ,
    70 : "2014-regionals-workout1-rank" ,
    71 : "2014-regionals-workout1-score" ,
    72 : "2014-regionals-workout2-rank" ,
    73 : "2014-regionals-workout2-score" ,
    74 : "2014-regionals-workout3-rank" ,
    75 : "2014-regionals-workout3-score" ,
    76 : "2014-regionals-workout4-rank" ,
    77 : "2014-regionals-workout4-score" ,
    78 : "2014-regionals-workout5-rank" ,
    79 : "2014-regionals-workout5-score" ,
    80 : "2014-regionals-workout6-rank" ,
    81 : "2014-regionals-workout6-score" ,
    82 : "2014-regionals-workout7-rank" ,
    83 : "2014-regionals-workout7-score",
    84 : "2012-games-workout1-rank",
    85 : "2012-games-workout2-rank",
    86 : "2012-games-workout3-rank",
    87 : "2012-games-workout4-rank",
    88 : "2012-games-workout5-rank",
    89 : "2012-games-workout6-rank",
    90 : "2012-games-workout7-rank",
    91 : "2012-games-workout8-rank",
    92 : "2012-games-workout9-rank",
    93 : "2012-games-workout10-rank",
    94 : "2012-games-workout11-rank",
    95 : "2012-games-workout12-rank",
    96 : "2012-games-workout13-rank",
    97 : "2012-games-workout14-rank",
    98 : "2012-games-workout15-rank",
    99 : "2013-games-workout1-rank",
    100 : "2013-games-workout2-rank",
    101 : "2013-games-workout3-rank",
    102 : "2013-games-workout4-rank",
    103 : "2013-games-workout5-rank",
    104 : "2013-games-workout6-rank",
    105 : "2013-games-workout7-rank",
    106 : "2013-games-workout8-rank",
    107 : "2013-games-workout9-rank",
    108 : "2013-games-workout10-rank",
    109 : "2013-games-workout11-rank",
    110 : "2013-games-workout12-rank",
    111 : "2014-games-workout1-rank",
    112 : "2014-games-workout2-rank",
    113 : "2014-games-workout3-rank",
    114 : "2014-games-workout4-rank",
    115 : "2014-games-workout5-rank",
    116 : "2014-games-workout6-rank",
    117 : "2014-games-workout7-rank",
    118 : "2014-games-workout8-rank",
    119 : "2014-games-workout9-rank",
    120 : "2014-games-workout10-rank",
    121 : "2014-games-workout11-rank",
    122 : "2014-games-workout12-rank",
    123 : "2014-games-workout13-rank"}

# Standards taken from Starting Strength by Mark Rippetoe
# http://startingstrength.com/files/standards.pdf
SS_SQUAT_M = [[78,144,174,240,320],
    [84,155,190,259,346],
    [91,168,205,278,369],
    [101,188,230,313,410],
    [110,204,250,342,445],
    [119,220,269,367,479],
    [125,232,285,387,504],
    [132,244,301,409,532],
    [137,255,311,423,551],
    [141,261,319,435,567],
    [144,267,326,445,580],
    [157,272,332,454,593]]

SS_WEIGHTS_M = [114,123,132,148,165,181,198,220,242,275,319,320]

df_squat_m = pd.DataFrame(SS_SQUAT_M, index=SS_WEIGHTS_M)

def _mongo_connection(host, database, collection, clean):
    client = pymongo.MongoClient()
    db = client[database]
    col = db[collection]

    if clean == True:
        col = col.find({ "2014-open-overall-rank" : {"$gt" : 0},
                        "Snatch": {"$gt" : 200, "$lt" : 350},
                        "Back Squat" : {"$gt" : 200, "$lt" : 600},
                        "Clean &amp; Jerk" : {"$gt" : 200, "$lt" : 600},
                        "Fran"  : {"$gt" : 60, "$lt" : 600} },
                      { "2014-open-overall-rank" : 1,
                        "Snatch": 1,
                        "Back Squat" : 1,
                        "Clean &amp; Jerk" : 1,
                        "Fran"  : 1 }).sort(
                          "athlete_id", pymongo.DESCENDING)

    return col


def initialize_df(host='localhost', database='crossfitSoup', collection='athleteRankings', clean=False, query={}, no_id=True):
    ''' Convert MongoDB to Pandas DataFrame. Solution obtained from
    http://stackoverflow.com/questions/16249736/how-to-import-data-from-mongodb-to-pandas

    # Connect to MongoDB
    col = _mongo_connection(host, database, collection, clean)

    # Make a query to the specific DB and Collection
    cursor = col.find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    Alternatively this could be convered once to a Pandas DF and saved to file
    df.to_pickle(file_name)
    and later loaded back with
    df = pd.read_pickle(file_name)
    '''
    df = pd.read_pickle('data/crossfit_data.pkl')

    #print(df.info(verbose=True))

    # Delete the _id
    if no_id:
        del df['_id']

    return df
