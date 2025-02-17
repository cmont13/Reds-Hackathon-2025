import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
from scipy import stats

lahman = pd.read_csv('lahman_people.csv')

savant = pd.read_csv('savant_data_2021_2023.csv')

sample = pd.read_csv('sample_submission.csv'
sample_players = sample['PLAYER_ID']

"""Total Appearances"""
appearances = savant[['batter', 'pitcher', 'game_date', 'game_pk', 'times_faced']]

appearances['season'] = appearances.apply(lambda x : int(x['game_date'].split('-')[0]), axis=1)

batters = pd.DataFrame(set(appearances['batter'])).rename(columns={0:'batter'})

batters = batters.merge(appearances[appearances['season'] == 2021].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['batter'])['times_faced'].sum().reset_index(), on='batter', how='left')
batters = batters.merge(appearances[appearances['season'] == 2022].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['batter'])['times_faced'].sum().reset_index(), on='batter', how='left', suffixes=['_2021', '_2022'])
batters = batters.merge(appearances[appearances['season'] == 2023].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['batter'])['times_faced'].sum().reset_index(), on='batter', how='left')
batters.rename(columns={'batter':'player', 'times_faced_2021':'pa_2021', 'times_faced_2022':'pa_2022', 'times_faced':'pa_2023'}, inplace=True)
batters['role'] = 1

pitchers = pd.DataFrame(set(appearances['pitcher'])).rename(columns={0:'pitcher'})

pitchers = pitchers.merge(appearances[appearances['season'] == 2021].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['pitcher'])['times_faced'].sum().reset_index(), on='pitcher', how='left')
pitchers = pitchers.merge(appearances[appearances['season'] == 2022].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['pitcher'])['times_faced'].sum().reset_index(), on='pitcher', how='left', suffixes=['_2021', '_2022'])
pitchers = pitchers.merge(appearances[appearances['season'] == 2023].groupby(['batter', 'pitcher', 'game_pk'])['times_faced'].max().reset_index().groupby(['pitcher'])['times_faced'].sum().reset_index(), on='pitcher', how='left')
pitchers.rename(columns={'pitcher':'player', 'times_faced_2021':'bf_2021', 'times_faced_2022':'bf_2022', 'times_faced':'bf_2023'}, inplace=True)
pitchers['role'] = 0

players = pd.concat([batters, pitchers]).fillna(0)

players['appearances_2021'] = players['pa_2021'] + players['bf_2021']
players['appearances_2022'] = players['pa_2022'] + players['bf_2022']
players['appearances_2023'] = players['pa_2023'] + players['bf_2023']
players = players[['player', 'role', 'appearances_2021', 'appearances_2022', 'appearances_2023']]

players['avg_appearances_2021_2022'] = (players['appearances_2021']  + players['appearances_2022']) / 2
players['avg_appearances_2022_2023'] = (players['appearances_2022']  + players['appearances_2023']) / 2

players['trend_appearances_2021_2022'] = (players['appearances_2022'] > players['appearances_2021']).astype(int)
players['trend_appearances_2022_2023'] = (players['appearances_2023'] > players['appearances_2022']).astype(int)

players.to_csv('playing_time.csv', index=False)

"""Age / Time in MLB / Height / Weight"""
lahman['debutYear'] = lahman.apply(lambda x : int(x['debut'].split('-')[0]), axis=1)

lahman['age_at_start_of_2023'] = 2022 - lahman['birthYear']
lahman['age_at_start_of_2024'] = 2023 - lahman['birthYear']

lahman['years_in_mlb_by_2023'] = 2023 - lahman['debutYear']
lahman['years_in_mlb_by_2024'] = 2024 - lahman['debutYear']

players_info = lahman[['player_mlb_id', 'height', 'weight', 'age_at_start_of_2023', 'age_at_start_of_2024', 'years_in_mlb_by_2023', 'years_in_mlb_by_2024']]

players_info.rename(columns={'player_mlb_id':'player'}, inplace=True)

players_info = players_info.dropna()

players_info = pd.concat([players_info, players_info])

players_info['role'] = 0

players_info.iloc[:(len(players_info) // 2), players_info.columns.get_loc('role')] = 1
players_info.iloc[(len(players_info) // 2):, players_info.columns.get_loc('role')] = 0

players_info.to_csv('player_info.csv', index=False)

"""Long Stretches of Missed Games / Games Played"""
def stringToDate(dateString):
  splitDate = dateString.split('-')
  return date(int(splitDate[0]), int(splitDate[1]), int(splitDate[2]))

savant.rename(columns={'game_date':'game_date_string'}, inplace=True)

savant['game_date'] = savant.apply(lambda x : stringToDate(x['game_date_string']), axis=1)

batter_games = savant.groupby(['batter'])['game_date'].agg(set).reset_index()
batter_games.rename(columns={'batter':'player'}, inplace=True)
batter_games['role'] = 1

pitcher_games = savant.groupby(['pitcher'])['game_date'].agg(set).reset_index()
pitcher_games.rename(columns={'pitcher':'player'}, inplace=True)
pitcher_games['role'] = 0

player_games = pd.concat([batter_games, pitcher_games], ignore_index=True)
player_games.rename(columns={'game_date':'game_date_list'}, inplace=True)

player_games['game_date_list'] = player_games.apply(lambda x : sorted(list(x['game_date_list'])), axis=1)

def getGamesInYear(game_list, year):
  games_in_year = list(filter(lambda x : x.year == year, game_list))
  return games_in_year

player_games['game_dates_2021'] = player_games.apply(lambda x : getGamesInYear(x['game_date_list'], 2021), axis=1)
player_games['game_dates_2022'] = player_games.apply(lambda x : getGamesInYear(x['game_date_list'], 2022), axis=1)
player_games['game_dates_2023'] = player_games.apply(lambda x : getGamesInYear(x['game_date_list'], 2023), axis=1)

player_games['games_played_2021'] = player_games.apply(lambda x : len(x['game_dates_2021']), axis=1)
player_games['games_played_2022'] = player_games.apply(lambda x : len(x['game_dates_2022']), axis=1)
player_games['games_played_2023'] = player_games.apply(lambda x : len(x['game_dates_2023']), axis=1)

player_games['games_played_2021_2022'] = player_games['games_played_2021'] + player_games['games_played_2022']
player_games['games_played_2022_2023'] = player_games['games_played_2022'] + player_games['games_played_2023']

def getTenDayMissedStretches(game_list):
  days_off_list = [(game_list[i + 1] - game_list[i]).days for i in range(1, len(game_list) - 1)]
  return sum(1 if days_off >= 10 else 0 for days_off in days_off_list)

player_games['ten_days_off_2021'] = player_games.apply(lambda x : getTenDayMissedStretches(x['game_dates_2021']), axis=1)
player_games['ten_days_off_2022'] = player_games.apply(lambda x : getTenDayMissedStretches(x['game_dates_2022']), axis=1)
player_games['ten_days_off_2023'] = player_games.apply(lambda x : getTenDayMissedStretches(x['game_dates_2023']), axis=1)

player_games['ten_days_off_2021_2022'] = player_games['ten_days_off_2021'] + player_games['ten_days_off_2022']
player_games['ten_days_off_2022_2023'] = player_games['ten_days_off_2022'] + player_games['ten_days_off_2023']

player_games = player_games[['player', 'role', 'ten_days_off_2021_2022', 'ten_days_off_2022_2023', 'games_played_2021_2022', 'games_played_2022_2023']]

player_games.to_csv('missed_game_stretches.csv', index=False)

"""Starter or Reliever"""
pitchers = savant[['pitcher', 'sp_indicator']]
pitchers = pitchers.groupby(['pitcher'])['sp_indicator'].agg(pd.Series.mode).reset_index()
pitchers['role'] = 0
pitchers.rename(columns={'pitcher':'player'}, inplace=True)
pitchers.to_csv('starter_reliever.csv', index=False)


"""OPS"""
reached_base = ['single', 'double', 'triple', 'home_run', 'walk', 'hit_by_pitch']
no_reached_base = ['double_play', 'field_error', 'field_out',
                  'fielders_choice', 'fielders_choice_out', 'force_out',
                  'grounded_into_double_play', 'other_out', 'sac_fly',
                  'sac_fly_double_play', 'strikeout', 'strikeout_double_play',
                  'triple_play']
at_bat = ['single', 'double', 'triple', 'home_run',
          'double_play', 'field_error', 'field_out',
          'fielders_choice', 'fielders_choice_out', 'force_out',
          'grounded_into_double_play', 'other_out','strikeout',
          'strikeout_double_play', 'triple_play']

plate_appearances = savant[(savant['events'].isin(reached_base)) | (savant['events'].isin(no_reached_base))].reset_index()

plate_appearances['player_type'] = plate_appearances[['batter', 'pitcher']].values.tolist()
plate_appearances = plate_appearances.explode('player_type').reset_index()
plate_appearances['player'] = plate_appearances.apply(lambda x : x['batter'] if x.name % 2 == 0 else x['pitcher'], axis=1)
plate_appearances['role'] = plate_appearances.apply(lambda x : 1 if x.name % 2 == 0 else 0, axis=1)
plate_appearances = plate_appearances[['player', 'role', 'game_date', 'events']]

plate_appearances['season'] = plate_appearances.apply(lambda x : int(x['game_date'].year), axis=1)

plate_appearances['reached_base'] = plate_appearances['events'].isin(reached_base).astype(int)

plate_appearances['obp_2021'] = plate_appearances[plate_appearances['season'] == 2021].groupby(['player', 'role'])['reached_base'].transform('mean')
plate_appearances['obp_2022'] = plate_appearances[plate_appearances['season'] == 2022].groupby(['player', 'role'])['reached_base'].transform('mean')
plate_appearances['obp_2023'] = plate_appearances[plate_appearances['season'] == 2023].groupby(['player', 'role'])['reached_base'].transform('mean')

player_stats = plate_appearances.groupby(['player', 'role'])['obp_2021'].agg('max').reset_index().merge(plate_appearances.groupby(['player', 'role'])['obp_2022'].agg('max'), on=['player','role'])
player_stats = player_stats.merge(plate_appearances.groupby(['player', 'role'])['obp_2023'].agg('max'), on=['player', 'role'])

at_bats = savant[savant['events'].isin(at_bat)].reset_index()

at_bats['player_type'] = at_bats[['batter', 'pitcher']].values.tolist()
at_bats = at_bats.explode('player_type').reset_index()
at_bats['player'] = at_bats.apply(lambda x : x['batter'] if x.name % 2 == 0 else x['pitcher'], axis=1)
at_bats['role'] = at_bats.apply(lambda x : 1 if x.name % 2 == 0 else 0, axis=1)
at_bats = at_bats[['player', 'role', 'game_date', 'events']]

at_bats['season'] = at_bats.apply(lambda x : int(x['game_date'].year), axis=1)

at_bats['slugging_value'] = (4 * (at_bats['events'] == 'home_run').astype(int)) + (3 * (at_bats['events'] == 'triple').astype(int)) + (2 * (at_bats['events'] == 'double').astype(int)) + (at_bats['events'] == 'single').astype(int)

at_bats['slg_2021'] = at_bats[at_bats['season'] == 2021].groupby(['player', 'role'])['slugging_value'].transform('mean')
at_bats['slg_2022'] = at_bats[at_bats['season'] == 2022].groupby(['player', 'role'])['slugging_value'].transform('mean')
at_bats['slg_2023'] = at_bats[at_bats['season'] == 2023].groupby(['player', 'role'])['slugging_value'].transform('mean')

player_stats = player_stats.merge(at_bats.groupby(['player', 'role'])['slg_2021'].agg('max'), on=['player', 'role'])
player_stats = player_stats.merge(at_bats.groupby(['player', 'role'])['slg_2022'].agg('max'), on=['player', 'role'])
player_stats = player_stats.merge(at_bats.groupby(['player', 'role'])['slg_2023'].agg('max'), on=['player', 'role'])

player_stats['ops_2021'] = player_stats['obp_2021'] + player_stats['slg_2021']
player_stats['ops_2022'] = player_stats['obp_2022'] + player_stats['slg_2022']
player_stats['ops_2023'] = player_stats['obp_2023'] + player_stats['slg_2023']

player_stats.fillna(0, inplace=True)

player_stats.to_csv('player_stats.csv', index=False)


"""Combining Dataframes"""
appearances = pd.read_csv('playing_time.csv')
missed_time = pd.read_csv('missed_game_stretches.csv')
player_info = pd.read_csv('player_info.csv')
player_stats = pd.read_csv('player_stats.csv')
pitcher_statcast = pd.read_csv('player_statcast_summary.csv')
ff_velo = pd.read_csv('ff_velo.csv')
ex_velo = pd.read_csv('exit_velo.csv')
starter_reliever = pd.read_csv('starter_reliever.csv')
pitcher_changes = pd.read_csv('pitchers_spin_rate_with_changes.csv')
batter_statcast = pd.read_csv('batterweightedavgs.csv').drop(columns=['Unnamed: 0'])

pitcher_statcast.rename(columns={'Pitcher':'player'}, inplace=True)
pitcher_statcast['role'] = 0

batter_statcast.rename(columns={'batter':'player'}, inplace=True)
batter_statcast['role'] = 1

pitcher_changes.rename(columns={'pitcher':'player'}, inplace=True)
pitcher_changes['role'] = 0

totals_df = player_info.merge(missed_time, on=['player', 'role'], how='left')
totals_df = totals_df.merge(appearances, on=['player', 'role'], how='left')
totals_df = totals_df.merge(player_stats, on=['player', 'role'], how='left')
totals_df = totals_df.merge(pitcher_statcast, on=['player', 'role'], how='left')
totals_df = totals_df.merge(batter_statcast, on=['player', 'role'], how='left')
totals_df = totals_df.merge(pitcher_changes, on=['player', 'role'], how='left')
totals_df = totals_df.merge(ff_velo, on=['player', 'role'], how='left')
totals_df = totals_df.merge(starter_reliever, on=['player', 'role'], how='left')

totals_df = pd.read_csv('totalsDF.csv')
ex_velo = pd.read_csv('exit_velo.csv')
totals_df = totals_df.merge(ex_velo, on=['player', 'role'], how='left')

totals_df.fillna(0, inplace=True)

totals_df.to_csv('totalsDF.csv', index=False)
