import requests
import pandas as pd
import base64
from pandas import json_normalize 
import json
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import math 
from datatransformer import DataTransformer

engine = create_engine('postgresql://postgres:Password@localhost:5432/wyscout')


def get_headers(client_id, client_secret):
    # Encode the credentials in Base64
    credentials = f'{client_id}:{client_secret}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    # Construct the headers for authentication
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }
    return headers

def get_competitions(area_code, headers):
    url = 'https://apirest.wyscout.com/v3/competitions'
    response = requests.get(url, headers=headers, params={'areaId': area_code})

    if response.status_code == 200:
        print("Successfully retrieved competitions data")
        return response.json()
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return None
    

def get_competition_dataframe(area_code, headers):
    found_competitions = []
    competitions = get_competitions(area_code, headers)

    if competitions is None:
        return None

    competition_details = []

    for competition in competitions.get('competitions', []):
        found_competitions.append(competition)

        competition_detail = competition.copy()
        
        area_detail = competition['area'].copy()
        area_detail['area_name'] = area_detail.pop('name')  # Rename 'name' to 'area_name'
        
        competition_detail.update(area_detail)
        del competition_detail['area']
        competition_details.append(competition_detail)

    competition_df = pd.DataFrame(competition_details)

    return competition_df

client_id = ''
client_secret = ''


def get_seasons(wyId, headers):
    url = f'https://apirest.wyscout.com/v3/competitions/{wyId}/seasons'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return None

def get_seasons_dataframe(competition_df, headers):
    # If competition_df is empty or None, exit early
    if competition_df is None or competition_df.empty:
        print("competition_df is None or empty. Exiting.")
        return None

    # Initialize a list to hold seasons details
    seasons_details = []

    # Loop through each row in the competition_df to get the wyId of each competition
    for _, row in competition_df.iterrows():
        wyId = row['wyId']

        # Get the seasons data using the wyId
        seasons_data = get_seasons(wyId, headers)
        
        if seasons_data is None:
            print(f"No seasons data found for wyId: {wyId}")
            continue

        for season_info in seasons_data.get('seasons', []):
            # Get the 'season' dictionary and add it to a new dictionary with other details
            season = season_info['season']
            season_detail = {
                'competition_wyId': wyId,
                'seasonId': season_info.get('seasonId'),
                'season_wyId': season.get('wyId'),
                'name': season.get('name'),
                'startDate': season.get('startDate'),
                'endDate': season.get('endDate'),
                'active': season.get('active'),
                'competitionId': season.get('competitionId'),
            }
            
            seasons_details.append(season_detail)

    # Create a DataFrame from the seasons details list
    seasons_df = pd.DataFrame(seasons_details)

    return seasons_df

def get_matches(seasonId, headers):
    url = f'https://apirest.wyscout.com/v3/seasons/{seasonId}/matches'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data for seasonId {seasonId}. HTTP Status code: {response.status_code}")
        time.sleep(1)

        return None

def get_matches_dataframe(seasons_df, headers):
    # If seasons_df is empty or None, exit early
    if seasons_df is None or seasons_df.empty:
        print("seasons_df is None or empty. Exiting.")
        return None

    # Initialize a list to hold matches details
    matches_details = []

    # Loop through each row in the seasons_df to get the seasonId of each season
    for _, row in seasons_df.iterrows():
        seasonId = row['seasonId']
        
        # Get the matches data using the seasonId
        matches_data = get_matches(seasonId, headers)
        
        if matches_data:
            for match in matches_data.get('matches', []):
                match_detail = match.copy()
                match_detail['seasonId'] = seasonId
                matches_details.append(match_detail)
        else:
            print(f"No matches data found for seasonId: {seasonId}")

    # Create a DataFrame from the matches details list
    matches_df = pd.DataFrame(matches_details)
    return matches_df

def get_teams(seasonId, headers):
    url = f'https://apirest.wyscout.com/v3/seasons/{seasonId}/teams'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data for seasonId {seasonId}. HTTP Status code: {response.status_code}")
        time.sleep(1)
        return None
        


def get_teams_dataframe(seasons_df, headers):
    # If seasons_df is empty or None, exit early
    if seasons_df is None or seasons_df.empty:
        print("seasons_df is None or empty. Exiting.")
        return None

    # Initialize a list to hold teams details
    teams_details = []

    # Loop through each row in the seasons_df to get the seasonId of each season
    for _, row in seasons_df.iterrows():
        seasonId = row['seasonId']
        
        # Get the teams data using the seasonId
        teams_data = get_teams(seasonId, headers)
        
        if teams_data:
            for team in teams_data.get('teams', []):
                team_detail = team.copy()
                
                # Unpack the nested 'area' dictionary into separate keys and values
                team_detail.update(team['area'])
                del team_detail['area']
                
                # Extracting 'name' and 'wyId' from the 'children' list
                children = team.get('children', [])
                team_detail['children_names'] = [child.get('name') for child in children]
                team_detail['children_wyIds'] = [child.get('wyId') for child in children]
                
                team_detail['seasonId'] = seasonId
                teams_details.append(team_detail)
        else:
            print(f"No teams data found for seasonId: {seasonId}")

    # Create a DataFrame from the teams details list
    teams_df = pd.DataFrame(teams_details)
    return teams_df

def get_players_for_season(season_wy_id, limit=100):
    all_players_data = []
    url = f'https://apirest.wyscout.com/v3/seasons/{season_wy_id}/players'
    credentials = f'{client_id}:{client_secret}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    # Construct the headers for authentication
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }

    page_count = 1
    current_page = 0
    while current_page < page_count:
        params = {'limit': limit, 'page': current_page + 1}

        # Make the request
        response = requests.get(url, params=params, headers=headers)

        # Raise an exception if the request was not successful
        if response.status_code != 200:
            response.raise_for_status()

        # Append the players data
        players_data = response.json()
        all_players_data.extend(players_data['players'])

        # Update the page count and current page
        page_count = players_data['meta']['page_count']
        current_page += 1

    return all_players_data

def get_players_for_all_seasons(season_wy_ids, limit=100):
    all_players_data = []
    for season_wy_id in season_wy_ids:
        print(f"Fetching players for season ID: {season_wy_id}")
        players_data = get_players_for_season(season_wy_id, limit)  
        all_players_data.append({'seasonId': season_wy_id, 'players': players_data})

    return all_players_data

def organize_all_players_data(all_players_data):
    players_data = []
    # Extracting players' data from all seasons
    for season_data in all_players_data:
        players_data.extend(season_data['players'])

    return organize_players_data(players_data)

def organize_players_data(players_data):
    # Creating a list to hold the information
    player_data = []

    # Iterating through the players and extracting the information
    for player in players_data:
        wyId = player['wyId']
        shortName = player['shortName']
        firstName = player['firstName']
        middleName = player['middleName']
        lastName = player['lastName']
        height = player['height']
        weight = player['weight']
        birthDate = player['birthDate']
        birthCountry = player['birthArea']['name']
        passportCountry = player['passportArea']['name']
        role = player['role']['name']
        foot = player['foot']
        currentTeamId = player['currentTeamId']
        currentNationalTeamId = player['currentNationalTeamId']
        gender = player['gender']
        status = player['status']
        image_url = player['imageDataURL']

        # Appending the extracted information to the list
        player_data.append([wyId, shortName, firstName, middleName, lastName, height, weight, birthDate, birthCountry, passportCountry, role, foot, currentTeamId, currentNationalTeamId, gender, status, image_url])

    # Creating the DataFrame
    PLAYERS = pd.DataFrame(player_data, columns=['wyId', 'shortName', 'firstName', 'middleName', 'lastName', 'height', 'weight', 'birthDate', 'birthCountry', 'passportCountry', 'role', 'foot', 'currentTeamId', 'currentNationalTeamId', 'gender', 'status', 'image_url'])
    return PLAYERS

# Function to flatten individual event
def flatten_event(event, match_id):
    # Specified keys to extract from the event dictionary
    keys_to_flatten = ['type', 'location', 'team', 'opponentTeam', 'player', 'pass', 
                       'possession', 'shot', 'groundDuel', 'aerialDuel', 'infraction', 'carry']
    
    # Creating a new dictionary with the extracted data
    extracted_data = {key: event.get(key, None) for key in keys_to_flatten}
    extracted_data['matchId'] = match_id
    extracted_data['videoTimestamp'] = event.get('videoTimestamp', None)
    
    # Flattening the dictionary to a dataframe
    flat_event = json_normalize(extracted_data)
    return flat_event
            
def get_match_events(matches_df, client_id, client_secret, season_ids, country, match_dict):
    
    # Looping through each season ID
    for season_id in season_ids:
        print(f"Processing season ID: {season_id}")

        # Getting matches pertaining to the current season ID
        group = matches_df[matches_df['seasonId'] == season_id]
        existing_match_ids = []  # Initialize to empty list

        try:
            query = f'SELECT DISTINCT "matchId" FROM {country}_events'
            print(f"Executing query: {query}")  # Debugging line
            
            # Using 'with' ensures that the connection is closed after this block is executed
            with engine.connect() as conn:
                existing_match_ids = pd.read_sql(query, conn)['matchId'].tolist()
                
            print(f"List of distinct match IDs: {existing_match_ids}")
            
        except ProgrammingError as e:
            print(f"An error occurred: {e}")  # Debugging line
            print("The table probably does not exist. Continuing...")
            
        except Exception as e:
            print(f"An unexpected error occurred: {e}")  # Debugging line for any other exception
            print("Continuing...")

        # Looping through each match ID
        for match_wy_id in group['matchId']:
            # Check if matchId is already in the database
            if match_wy_id in existing_match_ids:
                print(f"  Skipping match ID: {match_wy_id} (already in database)")
                continue

            print(f"  Processing match ID: {match_wy_id}")

            try:
                # Formulating the endpoint URL
                url = f'https://apirest.wyscout.com/v3/matches/{match_wy_id}/events'

                # Encoding the credentials in Base64
                credentials = f'{client_id}:{client_secret}'
                encoded_credentials = base64.b64encode(credentials.encode()).decode()

                # Setting up the headers for authentication
                headers = {
                    'Authorization': f'Basic {encoded_credentials}'
                }

                # Sending the request
                response = requests.get(url, headers=headers)

                # If the request was successful
                if response.status_code == 200:
                    match_event_data = response.json()
                    
                    # Checking if there are events in the response data
                    if 'events' in match_event_data:
                        
                        # Creating a list of dataframes by flattening each event data
                        match_events_df_list = [flatten_event(event, match_wy_id) for event in match_event_data['events']]
                        
                        # Concatenating all dataframes to create a single dataframe for the current match
                        all_match_events_df = pd.concat(match_events_df_list, ignore_index=True)
                        all_match_events_df = DataTransformer.apply_transformations(all_match_events_df, match_dict)
                        all_match_events_df.to_sql(f"{country}_events", engine, if_exists='append', index=False)
                        print(f"  Successfully processed match ID: {match_wy_id}")
                    else:
                        print(f"  No events found for match ID: {match_wy_id}")
                else:
                    print(f"  Error with match ID {match_wy_id}: {response.status_code}")
                    response.raise_for_status()
                    time.sleep(1)

            except requests.HTTPError as e:
                print(f"  Caught HTTPError for match ID {match_wy_id}: {e}")
                time.sleep(1)
                continue
        
        print(f"Processing for season ID {season_id} complete.")




def get_match_advanced_stats(match_wy_id, client_id, client_secret):
    # Construct the endpoint URL
    url = f'https://apirest.wyscout.com/v3/matches/{match_wy_id}/advancedstats'

    # Encode the credentials in Base64
    credentials = f'{client_id}:{client_secret}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    # Construct the headers for authentication
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }

    # Make the request
    response = requests.get(url, headers=headers)

    # Return the JSON data if the request was successful
    if response.status_code == 200:
        return response.json()
        print(response)
    else:
        # Print the match ID and error response
        print(f"Error processing matchId: {match_wy_id}. Response code: {response.status_code}. Response text: {response.text}")
        time.sleep(1)
        return None  # Return None so that the caller knows that there was an issue
    
def process_advanced_stats_to_dataframe(data):
    # Initialize an empty list to store the rows of the DataFrame
    rows = []

    if data is None:
        print("Data is None.")
        return None

    try:
        for match_id, match_data in data.items():
            # Initialize an empty dictionary to store the row data
            row = {}

            # Skip if match_data is None
            if match_data is None:
                print(f"Skipping matchId {match_id} as it has no data.")
                continue

            # Add the match_id to the row
            row['match_id'] = match_id

            # Extract home_id and away_id from the "teams" key if present
            if 'teams' in match_data:
                team_ids = list(match_data['teams'].keys())
                row['home_id'] = team_ids[0] if len(team_ids) > 0 else None
                row['away_id'] = team_ids[1] if len(team_ids) > 1 else None

            # Loop through all the categories (e.g., "general", "possession", etc.)
            for category, category_data in match_data.items():
                # Skip if the category is "matchId" or "teams" as these are special cases
                if category in ["matchId", "teams"]:
                    continue

                # Loop through teams (a and b)
                for idx, (team_id, team_stats) in enumerate(category_data.items()):
                    # Skip if team_stats is not a dictionary
                    if not isinstance(team_stats, dict):
                        continue

                    prefix = 'a' if idx == 0 else 'b'

                    # Populate row with statistics, adding the prefix 'a_' or 'b_'
                    # and the category as a second prefix
                    for stat, value in team_stats.items():
                        column_name = f"{category}_{stat}_{prefix}"
                        row[column_name] = value

            # Append the row to the list of rows
            rows.append(row)

        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)
        return df

    except Exception as e:
        # Moved variable declarations here to avoid UnboundLocalError in the exception block
        category = team_id = team_stats = "Unknown"
        print(f"An error occurred: {e}")
        print(f"Debug info: Data = {data}, Category = {category}, Team ID = {team_id}, Team Stats = {team_stats}, Type = {type(team_stats)}")


# Retrieve and process advanced stats data for a list of match IDs
def get_and_process_advanced_stats(matches_df, client_id, client_secret, country):
    # Loop through the match IDs and collect the advanced stats data
    for match_id in matches_df['matchId']:
        # Get the advanced stats for the current match
        advanced_stats_data = get_match_advanced_stats(match_id, client_id, client_secret)
        
        # Skip if no advanced stats were returned
        if not advanced_stats_data:
            print(f"No advanced stats data for matchId: {match_id}")
            continue
        
        # Process the current match data to a DataFrame and append to SQL database
        current_match_data = {match_id: advanced_stats_data}
        df_current = process_advanced_stats_to_dataframe(current_match_data)
        df_current.to_sql(f"{country}_advancedmatchstats", engine, if_exists='append', index=False)        
        # Here, df_current is saved to SQL within the process_advanced_stats_to_dataframe function
        print(f"Data for matchId: {match_id} has been processed and saved to SQL.")

    print("All match data has been processed and saved to SQL.")


def get_match_advanced_stats_players(match_wy_id, client_id, client_secret):
    # Construct the endpoint URL
    url = f'https://apirest.wyscout.com/v3/matches/{match_wy_id}/advancedstats/players'
    # Encode the credentials in Base64
    credentials = f'{client_id}:{client_secret}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    # Construct the headers for authentication
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }
    # Make the request
    response = requests.get(url, headers=headers)
    # Return the JSON data if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        # Print the match ID and error response
        print(f"Error processing matchId: {match_wy_id}. Response code: {response.status_code}. Response text: {response.text}")
        time.sleep(1)
        return None  # Return None so that the caller knows that there was an issue

def process_advanced_player_stats_to_dataframe(data):
    # Initialize an empty list to collect the flattened data
    flattened_data = []

    # Iterate over each match
    for match_id, match_data in data.items():
        print(f"Processing match_id: {match_id}")

        # Iterate over each player in the 'players' list
        for player in match_data['players']:
            print(f"  Processing player_id: {player['playerId']}")

            # Flatten sub-dictionaries for 'total', 'average', and 'percent'
            for key in ['total', 'average', 'percent']:
                if key in player:
                    for sub_key, value in player[key].items():
                        # Create a new key combining the parent and child keys
                        new_key = f"{key}_{sub_key}"
                        player[new_key] = value

            # Remove the original sub-dictionaries to avoid duplication
            player.pop('total', None)
            player.pop('average', None)
            player.pop('percent', None)

            # Add the match_id to the player dictionary
            player['match_id'] = match_id

            # Append the modified player dictionary to the flattened data list
            flattened_data.append(player)

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(flattened_data)
    return df

def get_and_process_advanced_stats_player(matches_df, client_id, client_secret, country):
    # Loop through the match IDs and collect the advanced stats data for players
    for match_id in matches_df['matchId']:
        # Get the advanced stats for the current match
        advanced_stats_data = get_match_advanced_stats_players(match_id, client_id, client_secret)
        
        # Skip if no advanced stats were returned
        if not advanced_stats_data:
            print(f"No advanced stats data for matchId: {match_id}")
            continue
        
        # Process the current match data to a DataFrame and append to SQL database
        current_match_data = {match_id: advanced_stats_data}
        df_current_player = process_advanced_player_stats_to_dataframe(current_match_data, country)
        df_current_player.to_sql(f"{country}_advancedplayersstats", engine, if_exists='append', index=False)        
      
        # Here, df_current_player is saved to SQL within the process_advanced_player_stats_to_dataframe function
        print(f"Player data for matchId: {match_id} has been processed and saved to SQL.")
    
    print("All player match data has been processed and saved to SQL.")


def get_formations(match_id, headers):
    """Get and parse formations for a given match_id."""
    try:
        # Construct the API URL
        url = f'https://apirest.wyscout.com/v3/matches/{match_id}/formations/'
        
        # Send the HTTP request and get the response
        response = requests.get(url, headers=headers)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Initialize an empty dictionary to hold the flattened data
        flattened_match = {}
        flattened_match['match_id'] = match_id
        
        # Iterate through home and away teams
        for team_id, team_data in data.items():
            # Determine if the team is home or away based on the order in the JSON
            team_type = 'home' if team_id == list(data.keys())[0] else 'away'
            
            # Initialize lists to hold player data for each half
            players_1h = []
            players_2h = []
            
            # Iterate through 1H and 2H
            for half, time_periods in team_data.items():
                for start_sec, schemes in time_periods.items():
                    for scheme_name, scheme_data in schemes.items():
                        # Extract scheme details and player list
                        scheme = scheme_data.get("scheme", "Unknown")
                        players = scheme_data.get("players", [])
                        
                        # Append this to the corresponding half
                        if half == '1H':
                            players_1h.append({"scheme": scheme, "players": players})
                        elif half == '2H':
                            players_2h.append({"scheme": scheme, "players": players})
            
            # Update the flattened match dictionary
            flattened_match[f"{team_type}_1h"] = players_1h
            flattened_match[f"{team_type}_2h"] = players_2h
        
        return flattened_match
    except Exception as e:
        # Print the exception message (for debugging purposes)
        time.sleep(1)
        print(f"Could not fetch formations for matchId: {match_id}. Error: {e}")
        return None
    
def fetch_match_details(matches_df, headers):

    # Initialize an empty dictionary to hold matchId and their formations
    match_details = {}

    # Iterate through the match IDs in matches_df
    for match_id in matches_df['matchId']:
        print(f"Processing matchId: {match_id}")

        # Construct the API URL
        url = f'https://apirest.wyscout.com/v3/matches/{match_id}'

        # Make the API request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON data
            formations_data = json.loads(response.text)

            # Add the formations data to the dictionary
            match_details[match_id] = formations_data
        else:
            time.sleep(1)
            print(f"Failed to get data for matchId: {match_id}")
            print(f"HTTP Status Code: {response.status_code}")
            print(f"Response Message: {response.text}")

    # Convert the dictionary to a DataFrame for further use
    match_details_df = pd.DataFrame.from_dict(match_details, orient='index')

    # Return the match details DataFrame
    return match_details_df

def get_player_contract_info(PLAYERS, client_id, client_secret):
    # Get the headers for authentication
    headers = get_headers(client_id, client_secret)
    
    # Create an empty list to store player contract info
    player_contract_infos = []

    # Loop through all wyIds in the PLAYERS dataframe
    for wyId in PLAYERS['wyId']:
        # Define the endpoint
        url = f"https://apirest.wyscout.com/v3/players/{wyId}/contractinfo"

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Append the player's contract info to the list
            player_contract_infos.append(response.json())
        else:
            # Append a None or some error indicator if the request was unsuccessful
            player_contract_infos.append(None)
            time.sleep(1)
        
        # Print the status of the current request (can be removed later)
        print(f"Request for wyId {wyId} returned status code {response.status_code}")

    # Convert the list of player contract info to a dataframe
    player_contract_info_df = pd.DataFrame(player_contract_infos)
    
    # Return the dataframe with all player's contract info
    return player_contract_info_df

def get_team_transfers(teams_df, client_id, client_secret):
    # Get the headers for authentication
    headers = get_headers(client_id, client_secret)
    
    # Create an empty list to store transfer data
    transfers_data = []

    # Loop through all wyIds in the teams_df dataframe
    for wyId in teams_df['wyId']:
        # Define the endpoint
        url = f"https://apirest.wyscout.com/v3/teams/{wyId}/transfers"

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Append the team's transfer data to the list (includes the wyId for reference)
            transfers_data.append({'wyId': wyId, 'transfers': response.json()['transfer']})
        else:
            # Append a None or some error indicator if the request was unsuccessful
            transfers_data.append({'wyId': wyId, 'transfers': None})
            time.sleep(1)

        # Print the status of the current request (can be removed later)
        print(f"Request for wyId {wyId} returned status code {response.status_code}")

    # Create a list to hold individual transfer records
    individual_transfers = []

    # Loop through the transfer data to create individual transfer records
    for transfer_data in transfers_data:
        for transfer in transfer_data['transfers'] or []:
            individual_transfer = transfer.copy()  # Copy individual transfer details
            individual_transfer['wyId'] = transfer_data['wyId']  # Add wyId to the transfer details
            individual_transfers.append(individual_transfer)  # Append to the list of individual transfers

    # Convert the list of individual transfer records to a dataframe
    transfers_df = pd.DataFrame(individual_transfers)
    
    # Return the dataframe with all team's transfer data
    return transfers_df

def get_full_squad(teams_df, client_id, client_secret, season=None):
    # Get the headers for authentication
    headers = get_headers(client_id, client_secret)
    
    # Create an empty list to store team data
    team_data_list = []

    # Loop through all wyIds in the teams_df dataframe
    for wyId in teams_df['wyId']:
        try:
            # Define the endpoint
            url = f"https://apirest.wyscout.com/v3/teams/{wyId}/squad"

            # Define the query parameters
            params = {'season': season}
            
            # Remove any parameters that are None to avoid issues with the request
            params = {k: v for k, v in params.items() if v is not None}

            # Make the GET request with the query parameters
            response = requests.get(url, headers=headers, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                # Get the squad data from the response
                squad_data = response.json()
                
                # Store the team data including the wyId, squad, coach, and staff
                team_data = {
                    'wyId': wyId,
                    'squad': squad_data.get('squad', []),
                    'coach': squad_data.get('coach', []),
                    'staff': squad_data.get('staff', []),
                }
                
                # Append the team data to the list
                team_data_list.append(team_data)
            else:
                # Print the status code if the request was unsuccessful
                print(f"Request for wyId {wyId} returned status code {response.status_code}")
                time.sleep(1)
        except Exception as e:
            # Print any errors that occur during the request
            print(f"An error occurred for wyId {wyId}: {e}")
            time.sleep(1)


    # Convert the list of team data to a dataframe
    squad_df = pd.DataFrame(team_data_list)
    
    # Return the dataframe with all the team data
    return squad_df


def get_player_details(PLAYERS, client_id, client_secret):
    # Get the headers for authentication
    headers = get_headers(client_id, client_secret)
    
    # Create an empty list to store player contract info
    player_contract_infos = []

    # Loop through all wyIds in the PLAYERS dataframe
    for wyId in PLAYERS['wyId']:
        try:
            # Define the endpoint
            url = f"https://apirest.wyscout.com/v3/players/{wyId}"
            print(f"processing {wyId}")


            # Make the GET request
            response = requests.get(url, headers=headers)

            # Check if the response is successful
            if response.status_code == 200:
                # Parse the JSON response
                player_data = response.json()
                
                # Append the player data to the list
                player_contract_infos.append(player_data)
            else:
                print(f"Failed to retrieve data for player with wyId {wyId}. Status code: {response.status_code}")
                time.sleep(1)

        except Exception as e:
            print(f"An error occurred while processing player with wyId {wyId}: {e}")
            time.sleep(1)

    # Create a dataframe from the list of player details
    players_details = pd.DataFrame(player_contract_infos)

    # Print the shape of the resulting dataframe for debugging
    print(f"The dataframe has been created with shape: {players_details.shape}")

    return players_details

# Function to convert a list of dictionaries to a list of JSON strings
def list_dicts_to_json(list_of_dicts):
    if isinstance(list_of_dicts, list):
        return [json.dumps(d) if isinstance(d, dict) else d for d in list_of_dicts]
    return list_of_dicts
