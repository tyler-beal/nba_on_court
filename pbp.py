"""

Which players are on the court?

"""
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import credentials
import warnings

def get_starting_lineup(quarter_df, quarter_players_df):
	"""
	Returns dataframe of the 10 players on the court for start of every quarter in play by play data
	
	:param quarter_df: general play by play NBA data subset to one quarter of one event_id
	:type quarter_df: pandas dataframe

	:param quarter_players_df: play by play data with player information subset to game and quarter
	:type quarter_players_df: pandas dataframe

	"""	
	starters_df = pd.DataFrame(columns = ['event_id','play_id', 'player_id'])
	if quarter_df['period'].iloc[0] == 1:
		starters_df = quarter_players_df.loc[quarter_players_df['play_event_id']==0][['event_id', 'play_id', 'player_id']].astype(int)
	else: 
		#get all players who played at all during the quarter
		all_players = quarter_players_df['player_id'].dropna().unique().tolist()
		starters=[]
		non_starters=[]
		#get all of the subs made in the quarter
		all_subs_df = quarter_players_df.loc[quarter_players_df['play_event_id']==10][['event_id','player_id','play_id','sequence']]	
		all_subs_dict = all_subs_df.to_dict('records')
		#from the first sub in the quarter, depending on which player was subbed in vs out, we can figure out who started
		for play in all_subs_dict:
			#this is in the case the same player is subbed in/out twice before one of the other starters
			#import pdb; pdb.set_trace()
			if play['player_id'] not in starters and play['player_id'] not in non_starters:
				all_players = [player for player in all_players if player != play['player_id']]
				if play['sequence'] == 1:
					non_starters.append(play['player_id'])
				elif play['sequence'] == 2:
					starters.append(play['player_id'])
			#if len(starters) == 10:
				#break
		if len(starters) != 10:
			#this next line is for players that played an entire quarter without being subbed. Steph sometimes does this
			starters.extend(all_players)
			if len(starters) > 10:
				warning_message = "WARNING: STARTING LINEUP IS GREATER THAN 10 FOR GAME: "+ str(play['event_id']) + " QUARTER: " + str(quarter_df['period'].iloc[0]) + ". Best guess starters applied."
				warnings.warn(warning_message)
				starters = best_guess_starters(quarter_players_df, all_players, starters, non_starters)
			elif len(starters) < 10:
				warning_message = "WARNING: STARTING LINEUP IS LESS THAN 10 FOR GAME: "+ str(play['event_id']) + " QUARTER: " + str(quarter_df['period'].iloc[0]) 
				warnings.warn(warning_message)
		starting_info = quarter_df.loc[quarter_df['play_event_id']==14]

		for starter in starters:
			new_starter = {'event_id':int(starting_info['event_id']), 'play_id':int(starting_info['play_id']), 'player_id':starter}
			starters_df = starters_df.append(new_starter, ignore_index=True)
	starters_df=starters_df.astype(int)
	return(starters_df)

def get_active_players(quarter_df, quarter_players_df, starters_df):
	"""
	Returns dataframe of the 10 players on the court for every play in play by play data
	
	:param quarter_df: general play by play NBA data subset to one quarter of one event_id
	:type quarter_df: pandas dataframe

	:param quarter_players_df: play by play data with player information subset to game and quarter
	:type quarter_players_df: pandas dataframe

	:param starters_df: dataframe containing players that started the quarter on the court
	:type starters_df: pandas dataframe 	

	"""	
	poc_df = pd.DataFrame(columns = ['event_id','play_id', 'player_id'])
	current_players=starters_df
	poc_df = poc_df = pd.concat([poc_df, current_players], ignore_index=True)
	if quarter_df['period'].iloc[0] == 1:
		after_starters = quarter_df.loc[quarter_df['play_event_id']!=0]
	else:
		after_starters = quarter_df.loc[(quarter_df['play_event_id']!=0) & (quarter_df['play_event_id']!=14)]
	after_starters = after_starters[['event_id','play_id','play_event_id']]
	q_dict = after_starters.to_dict('records')
	for play in q_dict:
		if play['play_event_id'] != 10:
			#if it's not a substitution, the current players list doens't change, so just update play_id
			current_players['play_id'] = play['play_id']
		elif play['play_event_id'] == 10:
			#if it is a substituion, we grab that play from the pbp_players_df and use sequence to find out who is in and who is out
			sub_df = quarter_players_df.loc[(quarter_players_df['event_id']==play['event_id']) & (quarter_players_df['play_id']==play['play_id'])]
			new_player_id = int(sub_df.loc[sub_df['sequence']==1]['player_id'].iloc[0])
			new_player = {'event_id':play['event_id'], 'play_id':play['play_id'], 'player_id':new_player_id}
			current_players = current_players.append(new_player, ignore_index=True)	
			#remove player from active_players
			remove_player_id = int(sub_df[sub_df['sequence']==2]['player_id'].iloc[0])
			current_players = current_players[current_players.player_id != remove_player_id]
			current_players['play_id'] = play['play_id']
		poc_df = pd.concat([poc_df, current_players], ignore_index=True)
	return(poc_df)	

def best_guess_starters(quarter_players_df, all_players, starters, non_starters):
	"""
	This function was made necessary by T.J Warren! In the 4th quarter of the game vs the Rockets on 2017/11/16,
	Mr. Warren, while residing on the bench since the 3rd quarter, got a technical foul. Mr. Warren remained on the bench for 
	the rest of the game. T.J Warren appeared on the play-by-play log without playing a single minute in the quarter. In 
	other words, T.J Warren is a true playmaker wherever he is in the gym
	
	Makes best guess on whichc unaccounted for player should be considered a starter

	:param quarter_players_df: play by play data with player information subset to game and quarter
	:type quarter_players_df: pandas dataframe

	:param all_players: list of all players who logged a play in the quarter
	:type all_players: list 

	:param starters: list of players we have identified as starters
	:type starters_df: list

	:param non_starters: list of players we have identified as non-starters
	:type non_starters: list

	"""
	#all players not accounted for through subs
	not_accounted_for = np.setdiff1d(all_players,non_starters)
	#get how many plays they were involved in
	play_count = quarter_players_df['player_id'].value_counts()
	play_count_dict = play_count.to_dict()
	play_count_dict = {player:play_count[player] for player in not_accounted_for}
	#of the players who showed up in the quarter that weren't subbed in or out, this guy has the least amount of plays and should be the candidate removed from best guess starting lineup
	remove_candidate = min(play_count_dict, key=play_count.get)
	best_guess_starters = [player for player in starters if player != remove_candidate]
	return(best_guess_starters)


def main():


	conn = "mysql+pymysql://{0}:{1}@{2}/{3}".format(credentials.dbuser, credentials.dbpass, credentials.dbhost, credentials.dbname)

	engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user=credentials.dbuser,
                               pw=credentials.dbpass,
                               db=credentials.dbname))		

	#Due to starting lineups only being available for quarter 1, I will be treating Q1 differently than the rest
	#read in data
	folder_path = sys.argv[1]
	pbp_file = sys.argv[2] 
	pbp_players_file = sys.argv[3]
	#read in .xlsx files with pandas
	pbp_df = pd.read_excel(folder_path+pbp_file, header=0)
	pbp_players_df = pd.read_excel(folder_path+pbp_players_file, header=0)

	pbp_df.sort_values(by = ['event_id','play_id'], inplace = True)
	pbp_players_df.sort_values(by = ['event_id','play_id'], inplace = True)
	#make empty df to append to
	on_court_df = pd.DataFrame(columns = ['event_id','play_id', 'player_id'])
	#dealing with one game at a time
	games=pbp_df.groupby('event_id')
	for game in games.groups:
		
		this_game=games.get_group(game)
		#deal with quarters individually
		quarters = this_game.groupby('period')
		for quarter in quarters.groups:
			
			quarter_df = quarters.get_group(quarter)
			#making subset of pbp_players_df
			quarter_players_df = pbp_players_df.loc[(pbp_players_df['event_id']==game) & (pbp_players_df['period']==quarter)]
			starters_df = get_starting_lineup(quarter_df, quarter_players_df)
			active_players = get_active_players(quarter_df, quarter_players_df, starters_df)
			on_court_df = pd.concat([on_court_df, active_players], ignore_index=True)
	on_court_df.to_csv('/home/bealt/coding_projects/Swish_Analytics/Swish Analytics - Data Engineer Project/Swish Analytics - Data Engineer Project/on_court.csv', index=False)

	on_court_df.to_sql('pbp_players_on_court', con = engine, if_exists = 'append', chunksize = 1000)
	pbp_players_on_court = pd.read_sql_table('pbp_players_on_court', engine)
	pbp_players_on_court = pbp_players_on_court[['event_id', 'play_id', 'player_id']]
	pbp_players_on_court.to_csv('/home/bealt/coding_projects/Swish_Analytics/Swish Analytics - Data Engineer Project/Swish Analytics - Data Engineer Project/on_court.csv', index=False) 

if __name__ == "__main__":
    main()
