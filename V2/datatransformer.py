import ast
import pandas as pd
import numpy as np
import math 


class DataTransformer:
        
    @classmethod
    def add_pass_features(cls, df):
        df['attacking_pass_accurate'] = None
        df['accurate_pass_speed'] = None

        # Iterate through rows
        for idx in range(len(df)):
            # Initialize variables for accurate pass counts and timestamps
            pass_accurate_count = 0
            first_timestamp = None
            last_timestamp = None

            if df.loc[idx, 'type.primary'] == 'shot':
                # Lookback variable to track previous rows
                lookback = 1

                while idx - lookback >= 0:
                    prev_row = df.iloc[idx - lookback]

                    # If previous row's event is an accurate pass
                    if prev_row['pass.accurate'] == True:
                        pass_accurate_count += 1

                        # Record the first and last timestamp encountered
                        if first_timestamp is None:
                            first_timestamp = prev_row['videoTimestamp']
                        last_timestamp = prev_row['videoTimestamp']

                    # If it's not an accurate pass, stop looking back
                    else:
                        break

                    # Increment lookback to check the next previous row
                    lookback += 1

                # Set the 'attacking_pass_accurate' column on the same row as the shot
                df.loc[idx, 'attacking_pass_accurate'] = pass_accurate_count

                # Calculate the 'accurate_pass_speed' if there's more than one pass
                if first_timestamp is not None and last_timestamp is not None:
                    df.loc[idx, 'accurate_pass_speed'] =  first_timestamp - last_timestamp
        return df
    
    @classmethod
    def compute_cumulative_red_cards(cls, df, match_dict):
        # Initialize the columns to zero
        df['home_red_cards'] = 0
        df['away_red_cards'] = 0

        # Create a dictionary to store cumulative red cards for each match
        cumulative_cards = {}

        # Iterate through the DataFrame to update the home_red_cards and away_red_cards columns
        for idx, row in df.iterrows():
            match_id = row.get('matchId')
            team_id = row.get('team.id')
            is_red_card = row.get('infraction.redCard')

            # Initialize match in cumulative_cards dictionary if not already present
            if match_id not in cumulative_cards:
                if match_id in match_dict:
                    home_id = match_dict[match_id]['home_id']
                    away_id = match_dict[match_id]['away_id']
                    cumulative_cards[match_id] = {'home_id': home_id, 'away_id': away_id, 'home_red_cards': 0, 'away_red_cards': 0}
                else:
                    continue

            # Update DataFrame with the latest counts before potentially updating them
            df.at[idx, 'home_red_cards'] = cumulative_cards[match_id]['home_red_cards']
            df.at[idx, 'away_red_cards'] = cumulative_cards[match_id]['away_red_cards']

            # Update the red card counters and the DataFrame if a red card is given
            if is_red_card == True:
                if team_id == cumulative_cards[match_id]['home_id']:
                    cumulative_cards[match_id]['home_red_cards'] += 1
                    df.at[idx, 'home_red_cards'] = cumulative_cards[match_id]['home_red_cards']
                elif team_id == cumulative_cards[match_id]['away_id']:
                    cumulative_cards[match_id]['away_red_cards'] += 1
                    df.at[idx, 'away_red_cards'] = cumulative_cards[match_id]['away_red_cards']
        
        return df
    
    @classmethod
    def compute_cumulative_goals(cls, df, match_dict):
        # Initialize the columns to zero
        df['home_goals'] = 0
        df['away_goals'] = 0

        # Create a dictionary to store cumulative goals for each match
        cumulative_goals = {}

        # Iterate through the DataFrame to update the home_goals and away_goals columns
        for idx, row in df.iterrows():
            match_id = row.get('matchId')
            team_id = row.get('team.id')
            is_goal = row.get('shot.isGoal')

            # Initialize match in cumulative_goals dictionary if not already present
            if match_id not in cumulative_goals:
                if match_id in match_dict:
                    home_id = match_dict[match_id]['home_id']
                    away_id = match_dict[match_id]['away_id']
                    cumulative_goals[match_id] = {'home_id': home_id, 'away_id': away_id, 'home_goals': 0, 'away_goals': 0}
                else:
                    continue

            # Update DataFrame with the latest counts before potentially updating them
            df.at[idx, 'home_goals'] = cumulative_goals[match_id]['home_goals']
            df.at[idx, 'away_goals'] = cumulative_goals[match_id]['away_goals']

            # Update the goal counters and the DataFrame if a goal is scored
            if is_goal == True:
                if team_id == cumulative_goals[match_id]['home_id']:
                    cumulative_goals[match_id]['home_goals'] += 1
                    df.at[idx, 'home_goals'] = cumulative_goals[match_id]['home_goals']
                elif team_id == cumulative_goals[match_id]['away_id']:
                    cumulative_goals[match_id]['away_goals'] += 1
                    df.at[idx, 'away_goals'] = cumulative_goals[match_id]['away_goals']
        
        return df
    
    @staticmethod
    def _add_feature_distance_center(df):
        # Compute the distance to the center
        df["distToCenter"] = np.abs(34 - df["possession.endLocation.y"])
        return df

    @staticmethod
    def _add_feature_distance_goal_line(df):
        # Compute the distance to the goal line
        df["distToGoalLine"] = 105 - df["possession.endLocation.x"]
        return df

    @staticmethod
    def add_feature_angle(df):
        # Distance to the post in y-direction
        df["dy"] = (df["distToCenter"] - 7.32 / 2)
        
        # If we have a negative angle, we want to set the distance to 0
        df["dy"].clip(lower=0, inplace=True)
        
        # Compute the angle to the closest post
        df["angle"] = df.apply(lambda row: math.degrees(math.atan2(row["dy"], row["distToGoalLine"])), axis=1)
        
        # Drop the temporary "dy" column
        df.drop("dy", axis=1, inplace=True)    
        return df

    
    @staticmethod
    def check_opportunity(lst):
        return 1 if 'opportunity' in lst else 0

    @staticmethod
    def check_extra_time(timestamp):
        return 1 if timestamp > 5400 else 0
    
    @staticmethod
    def is_second_half(timestamp):
        return 1 if timestamp > 2700 else 0    

    @staticmethod
    def first_15_flag(timestamp):
        return 1 if timestamp < 4500 else 0  
    
    @staticmethod
    def last_15_flag(timestamp):
        return 1 if timestamp > 2700 else 0      

    @staticmethod
    def get_first_keyword(type_secondary):
        return type_secondary[0] if type_secondary else None
    
    @staticmethod
    def generate_team_flag(row, match_dict):
        try:
            match_info = match_dict[row['matchId']]
            if row['team.id'] == match_info['home_id']:
                return 1  # Home team
            elif row['team.id'] == match_info['away_id']:
                return 0  # Away team
            else:
                print(f"Warning: team_id {row['team.id']} not found for matchId {row['matchId']}")
                return None  # Undefined team
        except KeyError:
            print(f"Warning: matchId {row['matchId']} not found in match_dict")
            return None  # Undefined match

    @staticmethod
    def is_counterattack(types_list):
        if isinstance(types_list, list) and types_list:
            return 1 if types_list[0] == 'counterattack' else 0 
        return 0 
    
    @classmethod
    def drop_unnecessary_columns(cls, df):
        columns_to_drop = [
            "shot", "groundDuel", "aerialDuel", "infraction", "carry", "team.name", "team.formation", 
            "opponentTeam.name", "opponentTeam.formation", "player.name", "player.position", 
            "pass.recipient.name", "pass.recipient.position", "possession.team.name", "possession.team.formation",
            "groundDuel.opponent.name", "groundDuel.opponent.position", "groundDuel.duelType", "groundDuel.opponent",
            "infraction.opponent.name", "infraction.opponent.position", "aerialDuel.opponent",
            "aerialDuel.opponent.name", "aerialDuel.opponent.position", "shot.goalkeeper.name", "infraction.opponent",
            "location", "shot.goalkeeper", "possession", 
            "possession.attack", "pass"
        ]
        
        # Dropping columns that are in the dataframe and in the columns_to_drop list
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        
        return df

    # Applying transformations
    @classmethod
    def apply_transformations(cls, df, match_dict):
        df['counterattack'] = df['possession.types'].apply(cls.is_counterattack)
        df['preceding_action'] = df['type.secondary'].shift(1).apply(cls.get_first_keyword)
        df['videoTimestamp'] = df['videoTimestamp'].astype(float)
        df['opportunity'] = df['type.secondary'].apply(cls.check_opportunity)
        df['extra_time'] = df['videoTimestamp'].apply(cls.check_extra_time)
        df['is_second_half'] = df['videoTimestamp'].apply(cls.is_second_half)
        df['first_15_flag'] = df['videoTimestamp'].apply(cls.first_15_flag)
        df['last_15_flag'] = df['videoTimestamp'].apply(cls.last_15_flag)
        df['team_flag'] = df.apply(lambda row: cls.generate_team_flag(row, match_dict), axis=1)
        df = cls._add_feature_distance_center(df)
        df = cls._add_feature_distance_goal_line(df)
        df = cls.add_feature_angle(df)
        df = cls.add_pass_features(df)
        df = cls.compute_cumulative_goals(df, match_dict)
        df = cls.compute_cumulative_red_cards(df, match_dict)
        df = cls.drop_unnecessary_columns(df)
        
        return df