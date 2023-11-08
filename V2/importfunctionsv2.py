import requests
import pandas as pd
import base64
from pandas import json_normalize 
import json
from sqlalchemy import create_engine
import math 
import time

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
        time.sleep(1)
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
        time.sleep(1)
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

def fetch_teams_and_save_to_json(seasons_df, client_id, client_secret, league_name):
    # Get unique wyIds
    unique_wyIds = seasons_df['seasonId'].unique()

    # Get headers for the request
    headers = get_headers(client_id, client_secret)
    
    # Initialize an empty list to store all teams data
    all_teams_data = []

    # Loop through each unique wyId to fetch the team data
    for wyId in unique_wyIds:
        # Construct the URL
        url = f'https://apirest.wyscout.com/v2/seasons/{wyId}/teams'

        # Make the API request
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Get the raw data from the response
            data = response.json()
            
            # Append the 'teams' list from the response data to the all_teams_data list
            if 'teams' in data:
                all_teams_data.extend(data['teams'])
        else:
            # Print error message if the request was not successful
            print(f"Failed to retrieve data for wyId: {wyId}. HTTP Status code: {response.status_code}")
            time.sleep(1)
    # Save all the team data to a JSON file using a one-liner
    with open(f"D:\\Wyscout\\Teams\\{league_name}_teams.json", 'w') as file:
        json.dump(all_teams_data, file)

    print("Data saved to all_teams_data.json")



def get_players_for_all_seasons(seasons_df, client_id, client_secret, league_name):

    all_players_data = []
    unique_wyIds = seasons_df['seasonId'].unique()

    for season_wy_id in unique_wyIds:
            # Construct the endpoint URL
            url = f'https://apirest.wyscout.com/v2/seasons/{season_wy_id}/players'

            # Get the headers for authentication
            credentials = f'{client_id}:{client_secret}'
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers = {
                'Authorization': f'Basic {encoded_credentials}'
            }

            # Make the API request
            response = requests.get(url, headers=headers)

            # Check if the request was successful
            if response.status_code == 200:
                # Get the raw data from the response
                data = response.json()
                # Append the 'players' list from the response data to the all_players_data list
                if 'players' in data:
                    all_players_data.extend(data['players'])
            else:
                # Print error message if the request was not successful
                print(f"Failed to retrieve data for season wyId: {season_wy_id}. HTTP Status code: {response.status_code}")
                time.sleep(1)
    # Save all the player data to a JSON file
    with open(f"D:\\Wyscout\\Players\\{league_name}_players.json", 'w') as file:
        json.dump(all_players_data, file)

    print("Data saved to all_players_data.json")


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

def get_matches_data(matches_df, client_id, client_secret, league_name):
    all_matches_data = []

    unique_matchIds = matches_df['matchId'].unique()

    for matchId in unique_matchIds:
        # Construct the endpoint URL
        url = f'https://apirest.wyscout.com/v2/matches/{matchId}'

        # Get the headers for authentication
        credentials = f'{client_id}:{client_secret}'
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            'Authorization': f'Basic {encoded_credentials}'
        }

        # Make the API request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the raw data from the response and add it to our list
            data = response.json()
            all_matches_data.append(data)
        else:
            # Print error message if the request was not successful
            print(f"Failed to retrieve data for matchId: {matchId}. HTTP Status code: {response.status_code}")
            time.sleep(1)
    # Save all the player data to a JSON file
    with open(f"D:\\Wyscout\\Matches\\{league_name}_matches.json", 'w') as file:
        json.dump(all_matches_data, file)

    print("Data saved to all_matches_data.json")


def get_events_data(matches_df, client_id, client_secret, league_name):
    all_events_data = []

    unique_matchIds = matches_df['matchId'].unique()

    for matchId in unique_matchIds:
        # Construct the endpoint URL
        url = f'https://apirest.wyscout.com/v2/matches/{matchId}/events'

        # Get the headers for authentication
        credentials = f'{client_id}:{client_secret}'
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            'Authorization': f'Basic {encoded_credentials}'
        }

        # Make the API request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the raw data from the response
            data = response.json()

            # Extract events data directly from the dictionary
            events_data = data.get('events', [])
            
            # Append events data to all_events_data
            all_events_data.extend(events_data)
        else:
            # Print error message if the request was not successful
            print(f"Failed to retrieve data for matchId: {matchId}. HTTP Status code: {response.status_code}")
            time.sleep(1)

    # Save all the event data to a JSON file
    with open(f"D:\\Wyscout\\Events\\{league_name}_events.json", 'w') as file:
        json.dump(all_events_data, file)

    print("Data saved to all_events_data.json")
