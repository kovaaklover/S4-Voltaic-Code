import csv
import statistics
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os

# KOVAAKs LEADERBOARD IDs
Leaderboard_ID = [
    29489, 24975, 29483, 24969,
    0, 0, 24967, 24968,
    43183, 43180, 0, 0,
    24979, 24971, 24980, 24973,


    29493, 24955, 29494, 24957,
    25063, 29403, 24952, 24958,
    43184, 43181, 26261, 29421,
    24960, 24964, 24962, 24953,

    29492, 25154, 29479, 23559,
    24333, 29412, 25151, 25176,
    43186, 43182, 26262, 29472,
    25177, 25150, 25181, 25160
]


# S4 RANK REQUIREMENTS
RankReq = [
    [0, 550, 650, 750, 850, 750, 850, 950, 1050, 940, 1040, 1120, 1270],
    [0, 500, 600, 700, 800, 600, 700, 800, 900, 800, 900, 1100, 1150],
    [0, 650, 750, 850, 950, 1000, 1100, 1200, 1300, 1280, 1380, 1460, 1580],
    [0, 1160, 1260, 1360, 1460, 1360, 1460, 1560, 1660, 1630, 1770, 1890, 2000],
    [0, 0, 0, 0, 0, 740, 830, 920, 1000, 880, 1020, 1150, 1230],
    [0, 0, 0, 0, 0, 660, 750, 850, 940, 940, 1080, 1150, 1230],
    [0, 2300, 2500, 3100, 3500, 3050, 3450, 3850, 4250, 3300, 3600, 3950, 4300],
    [0, 1300, 1600, 1900, 2200, 1650, 2050, 2450, 2850, 2500, 2850, 3250, 3650],
    [0, 2150, 2450, 2850, 3050, 2680, 2980, 3280, 3530, 3275, 3475, 3600, 3800],
    [0, 1900, 2200, 2500, 2800, 2450, 2700, 2950, 3200, 3000, 3250, 3500, 3750],
    [0, 0, 0, 0, 0, 2260, 2620, 2800, 3050, 3050, 3240, 3400, 3500],
    [0, 0, 0, 0, 0, 2800, 3000, 3200, 3400, 3400, 3600, 3700, 3825],
    [0, 620, 690, 760, 830, 810, 880, 950, 1020, 1080, 1160, 1200, 1330],
    [0, 780, 860, 950, 1040, 1030, 1130, 1220, 1300, 1300, 1430, 1500, 1600],
    [0, 450, 510, 560, 620, 550, 600, 650, 700, 680, 740, 780, 830],
    [0, 490, 550, 610, 680, 630, 670, 710, 760, 820, 920, 970, 1050]
]

NLimits = [950, 900, 1050, 1560, 0, 0,  3900, 2500, 3250, 3100, 0, 0, 900, 1130, 680, 750]

ILimits = [1150, 1000, 1400, 1760, 1080, 1030,  4650, 3250, 3780, 3450, 3300, 3600, 1090, 1380, 750, 810]

# S3 RANKS
Ranks = ["N/A", "Unranked", "Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Jade", "Master", "Grandmaster", "Nova", "Astra", "Celestial"]

# FUNCTION TO PROCESS EACH PAGE OF EACH LEADERBOARD (FUNCTION CALLED VIA THREADING)
def process_leaderboard(leaderboard_id, page, session, itera, Count, score_lock, Score_Dic, RankReq):
    result = []

    # API DATA PULL
    try:
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={leaderboard_id}&page={page}&max=100").json()
        print(f"Leaderboard {leaderboard_id}. Page: {page} data pull.")

        # ITERATE THROUGH ALL DATA ROWS (100 LEADERBOARD ENTRIES) IN THE API PULL
        for Data in r['data']:
            try:
                Steam_Name = Data['steamAccountName']
                Steam_ID = Data['steamId']
                Score = Data['score']

                VoltsN = 0
                VoltsI = 0
                VoltsA = 0


                # LOCK
                with score_lock:

                    # IF STEAM ID WAS NOT YET SEEN CREATE KEY AND SET VOLTS TO ZERO
                    if Steam_ID not in Score_Dic:
                        Score_Dic[Steam_ID] = [0] * (147)
                        Score_Dic[Steam_ID][99] = Steam_Name

                    # FOR NOVICE LEADERBOARDS
                    if itera == 1:

                        # ITERATE THROUGH RANKS
                        for iii in range(1, 5):
                            if iii == 4 and RankReq[Count][iii] <= Score:

                                VoltsN = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (NLimits[Count] - RankReq[Count][iii])
                                if VoltsN > 500:
                                    VoltsN = 500
                                Score_Dic[Steam_ID][48 + Count+0] = VoltsN

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count] = Score
                                VoltsN = (iii) * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][48 + Count+0] = VoltsN

                            elif iii==1:
                                Score_Dic[Steam_ID][Count] = Score
                                VoltsN = 0 + (Score - 0) * 100 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][48 + Count+0] = VoltsN

                        Score_Dic[Steam_ID][96] += VoltsN/12


                    # FOR INTERMEDIATE LEADERBOARD
                    elif itera == 2:

                        # ITERATE THROUGH RANKS
                        for iii in range(5, 9):
                            if iii == 8 and RankReq[Count][iii] <= Score:
                                VoltsI = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (ILimits[Count] - RankReq[Count][iii])
                                if VoltsI > 900:
                                    VoltsI = 900
                                Score_Dic[Steam_ID][48 + Count+16] = VoltsI

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count+16] = Score
                                VoltsI = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][48 + Count+16] = VoltsI

                            elif iii == 5:
                                Score_Dic[Steam_ID][Count+16] = Score
                                VoltsI = 0 + (Score - 0) * 500 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][48 + Count+16] = VoltsI

                        Score_Dic[Steam_ID][97] += VoltsI/16

                    # FOR ADVANCED LEADERBOARD
                    elif itera == 3:

                        # ITERATE THROUGH RANKS
                        for iii in range(9, 13):
                            if iii == 12 and RankReq[Count][iii] <= Score:
                                VoltsA = 1200
                                Score_Dic[Steam_ID][48 + Count+32] = VoltsA

                            elif RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][Count+32] = Score
                                VoltsA = iii * 100 + (Score - RankReq[Count][iii]) * 100 / (RankReq[Count][iii+1] - RankReq[Count][iii])
                                Score_Dic[Steam_ID][48 + Count+32] = VoltsA

                            elif iii == 9:
                                Score_Dic[Steam_ID][Count+32] = Score
                                VoltsA = 0 + (Score - 0) * 900 / (RankReq[Count][iii] - 0)
                                Score_Dic[Steam_ID][48 + Count+32] = VoltsA

                        Score_Dic[Steam_ID][98] += VoltsA/16

            except KeyError:
                continue
    except Exception as e:
        print(f"Error processing leaderboard {leaderboard_id} page {page}: {e}")
    return result


# Main code with threading and lock protection
Score_Dic = {}
score_lock = Lock()  # Create a lock for protecting shared resources

# START THREADER
with ThreadPoolExecutor(max_workers=10) as executor:
    Count = 0
    itera = 1
    futures = []
    session = requests.Session()

    # ITERATE THROUGH ALL LEADERBOARDS
    for i in range(len(Leaderboard_ID)):

        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={Leaderboard_ID[i]}&page=0&max=100").json()
        Max_Page = r.get('total', 0) // 100
     #   Max_Page=10

        # ITERATE THROUGH ALL LEADERBOARD PAGES AND SEND TO FUNCTION
        for ii in range(Max_Page + 1):
            futures.append(executor.submit(process_leaderboard, Leaderboard_ID[i], ii, session, itera, Count, score_lock, Score_Dic, RankReq))

        # LOCK CRITERIA (NEEDED)
        with score_lock:
            Count += 1
            if Count >= 16 and itera == 1:
                Count = 0
                itera = 2
            elif Count >= 16 and itera == 2:
                Count = 0
                itera = 3

    # PROCESS RESULTS
    for future in as_completed(futures):
        future.result()  # No need to handle this since the processing is done within the function

    session.close()


# ITERATE THROUGH ALL KEYS IN DICTIONARY
Count = 0
for key, values in Score_Dic.items():
    RankN = values[48:64]
    RankI = values[64:80]
    RankA = values[80:96]

    # CALCULATE RANK VOLTS
    RN = statistics.harmonic_mean([max(RankN[0:2]), max(RankN[2:4]), max(RankN[6:8]), max(RankN[8:10]), max(RankN[12:14]), max(RankN[14:16])])
    values[100] = RN

    RI = statistics.harmonic_mean([max(RankI[0:2]), max(RankI[2:4]), max(RankI[6:8]), max(RankI[8:10]), max(RankI[12:14]), max(RankI[14:16])])
    values[101] = RI

    RA = statistics.harmonic_mean([max(RankA[0:2]), max(RankA[2:4]), max(RankA[6:8]), max(RankA[8:10]), max(RankA[12:14]), max(RankA[14:16])])
    values[102] = RA

    # CALCULATE RANK FROM RANK VOLTS NOVICE
    for i in range(0, 5):

        # GET TASK RANKS
        for ii in range(0, 16):
            if values[ii+48] >= i * 100:
                values[110 + ii] = Ranks[i+1]
                values[126 + ii] = values[ii]

        # GET RANK NOVICE
        if values[100] >= i*100:
            values[103] = Ranks[i+1]

            if min([min(RankN[0:2]), min(RankN[2:4]), min(RankN[6:8]), min(RankN[8:10]), min(RankN[12:14]), min(RankN[14:16])]) >= i*100 and i > 0:
                values[103] = values[103] + " Complete"
            values[108] = values[103]

    # CALCULATE RANK FROM RANK VOLTS INTERMEDIATE
    for i in range(5, 9):

        # GET TASK RANKS
        for ii in range(0, 16):
            if values[ii+16+48] >= i * 100:
                values[110 + ii] = Ranks[i+1]
                values[126 + ii] = values[ii + 16]

        # GET RANK INTERMEDIATE
        if values[101] >= i*100:
            values[104] = Ranks[i+1]

            if min(RankI) >= i*100:
                values[104] = values[104] + " Complete"
            values[108] = values[104]

    # CALCULATE RANK FROM RANK VOLTS ADVANCED
    for i in range(9, 14):

        # GET TASK RANKS
        for ii in range(0, 16):
            if values[ii+32+48] >= i * 100:
                values[110 + ii] = Ranks[i+1]
                values[126 + ii] = values[ii + 32]

        # GET RANK ADVANCED
        if values[102] >= i*100:
            values[105] = Ranks[i+1]

            if min(RankA) >= i*100:
                values[105] = values[105] + " Complete"
            values[108] = values[105]

    # RANK WITHOUT COMPLETE
    if values[108].endswith(" Complete"):
        values[109] = values[108][:-9]  # Remove exactly 9 characters (the length of " Complete")
    else:
        values[109] = values[108]

    # MAKE IT SO THAT MASTER PLAYERS ARE ALWAYS WORSE THAN GM PLAYERS
    if values[102] < 900:
        values[98] = 0

    if values[101] < 500:
        values[97] = 0

    # COUNT OF RELEVANT ENTRIES
    if values[100] > 0 or values[101] > 0 or values[102] > 0:
        Count += 1

# SORT NOVICE VOLTS THEN INTERMEDIATE VOLTS THEN ADVANCED COMPLETE POINTS
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][98], item[1][97], item[1][96]), reverse=True))
Per = 0
for key, values in Score_Dic_S.items():
    if values[100] > 0 or values[101] > 0 or values[102] > 0:
        values[106] = Per+1
        values[107] = round(1 - Per / Count, 6)
        Per += 1

        # IF GREATER THAN GOLD MAX GOLD ENERGY
     #   if values[100] > 400:
     #       values[142] = 400
     #   else:
        values[142] = values[100]

        # IF GREATER THAN MASTER MAX MASTER ENERGY
    #    if values[101] > 800:
   #         values[143] = 800
   #     else:
        values[143] = values[101]
        values[144] = values[102]
        
        # IF LESS THAN MASTER AND GRANDMASTER SET ADVANCED ENERGY TO ZERO
        if values[101] < 800 and values[102] < 900:
            values[144] = 0

        # IF LESS THAN GOLD AND MASTER SET MASTER AND GRANDMASTER ENERGY TO ZERO
        if values[100] < 400 and values[101] < 800 and values[102] < 900:
            values[143] = 0
            values[144] = 0

# SORT NOVICE VOLTS THEN INTERMEDIATE VOLTS THEN ADVANCED ENERGY
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][144], item[1][143], item[1][142]), reverse=True))
Per = 0
for key, values in Score_Dic_S.items():
    if values[100] > 0 or values[101] > 0 or values[102] > 0:
        values[145] = Per+1
        values[146] = round(1 - Per / Count, 6)
        Per += 1
    
header = ['PlayerID',
'VT Pasu Rasp Novice','VT Bounceshot Novice','VT 1w6ts Rasp Novice','VT Multiclick 120 Novice',
'N/A','N/A','VT Smoothbot Novice','VT PreciseOrb Novice',
'VT Plaza Novice','VT Air Novice','N/A','N/A',
'VT psalmTS Novice','VT skyTS Novice','VT evaTS Novice','VT bounceTS Novice',

'VT Pasu Rasp Intermediate','VT Bounceshot Intermediate','VT 1w5ts Rasp Intermediate','VT Multiclick 120 Intermediate',
'VT AngleStrafe Intermediate','VT ArcStrafe Intermediate','VT Smoothbot Intermediate','VT PreciseOrb Intermediate',
'VT Plaza Intermediate','VT Air Intermediate','VT PatStrafe Intermediate','VT AirStrafe Intermediate',
'VT psalmTS Intermediate','VT skyTS Intermediate','VT evaTS Intermediate','VT bounceTS Intermediate',

'VT Pasu Rasp Advanced','VT Bounceshot Advanced','VT 1w3ts Rasp Advanced','VT Multiclick 180 Advanced',
'VT AngleStrafe Advanced','VT ArcStrafe Advanced','VT Smoothbot Advanced','VT PreciseOrb Advanced',
'VT Plaza Advanced','VT Air Advanced','VT PatStrafe Advanced','VT AirStrafe Advanced',
'VT psalmTS Advanced','VT skyTS Advanced','VT evaTS Advanced','VT bounceTS Advanced',

'VT Pasu Rasp Novice V','VT Bounceshot Novice V','VT 1w6ts Rasp Novice V','VT Multiclick 120 Novice V',
'N/A','N/A','VT Smoothbot Novice V','VT PreciseOrb Novice V',
'VT Plaza Novice V','VT Air Novice V','N/A','N/A',
'VT psalmTS Novice V','VT skyTS Novice V','VT evaTS Novice V','VT bounceTS Novice V',

'VT Pasu Rasp Intermediate V','VT Bounceshot Intermediate V','VT 1w5ts Rasp Intermediate V','VT Multiclick 120 Intermediate V',
'VT AngleStrafe Intermediate V','VT ArcStrafe Intermediate V','VT Smoothbot Intermediate V','VT PreciseOrb Intermediate V',
'VT Plaza Intermediate V','VT Air Intermediate V','VT PatStrafe Intermediate V','VT AirStrafe Intermediate V',
'VT psalmTS Intermediate V','VT skyTS Intermediate V','VT evaTS Intermediate V','VT bounceTS Intermediate V',

'VT Pasu Rasp Advanced V','VT Bounceshot Advanced V','VT 1w3ts Rasp Advanced V','VT Multiclick 180 Advanced V',
'VT AngleStrafe Advanced V','VT ArcStrafe Advanced V','VT Smoothbot Advanced V','VT PreciseOrb Advanced V',
'VT Plaza Advanced V','VT Air Advanced','VT PatStrafe Advanced V','VT AirStrafe Advanced V',
'VT psalmTS Advanced V','VT skyTS Advanced V','VT evaTS Advanced V','VT bounceTS Advanced V',

'Novice Complete Points', 'Intermediate Complete Points', 'Advanced Complete Points', 'Steam Name', 'Novice Energy',  'Intermediate Energy',  'Advanced Energy', 'Novice Rank',  'Intermediate Rank',  'Advanced Rank', 'Rank','Percentage','Max Rank','Base Rank',

'VT Pasu Rasp R','VT Bounceshot R','VT 1wXts Rasp R','VT Multiclick 180 R',
'VT AngleStrafe R','VT ArcStrafe R','VT Smoothbot R','VT PreciseOrb R',
'VT Plaza R','VT Air R','VT PatStrafe R','VT AirStrafe R',
'VT psalmTS R','VT skyTS R','VT evaTS R','VT bounceTS R',

'VT Pasu Rasp S','VT Bounceshot S','VT 1wXts Rasp S','VT Multiclick 180 S',
'VT AngleStrafe S','VT ArcStrafe S','VT Smoothbot S','VT PreciseOrb S',
'VT Plaza S','VT Air S','VT PatStrafe S','VT AirStrafe S',
'VT psalmTS S','VT skyTS S','VT evaTS S','VT bounceTS S',

'ADJ N E', 'ADJ I E', 'ADJ A E', 'E Rank', 'E Percentage',
]

header1 = [header[0]] + header[97:]

# CSV PRINT
#csv_file = 'output.csv'
#with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
#    writer = csv.writer(file)
#    writer.writerow(header)
#    for key, values in Score_Dic_S.items():
#        if values[100] > 0 or values[101] > 0 or values[102] > 0:
#            if values[99] is not None:
#                values[99] = values[99].encode('ascii', 'ignore').decode('ascii')
#            else:
#                values[99] = ''

#            writer.writerow([key] + values)
#            Per += 1

# GOOGLE SHEETS API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# JSON CREDENTIAL FILE PATH
creds_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS'))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

# AUTHORIZE THE CLIENT
client = gspread.authorize(creds)

# OPEN GOOGLE SHEET
sheet = client.open('S4_Voltaic').sheet1

# CLEAR EXISTING DATA IN GOOGLE SHEET
sheet.clear()

# WRITE HEADERS TO FIRST ROW
sheet.append_row(header1)

# SEND DATA FROM DICTIONARY TO ARRAY
rows_to_update = []
for key, values in Score_Dic_S.items():
    if values[100] > 0 or values[101] > 0 or values[102] > 0:
        if values[99] is not None:
            values[99] = values[99].encode('ascii', 'ignore').decode('ascii')
        else:
            values[99] = ''

        values=values[96:]

        # Add the row to the list
        rows_to_update.append([key] + values)

# UPDATE GOOGLE SHEET WITH ALL ARRAY DATA
start_cell = 'A2'
sheet.update(rows_to_update, start_cell)
