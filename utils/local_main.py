from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen

html = urlopen('https://cleaningtheglass.com/stats/league/summary')
soup = BeautifulSoup(html, 'html.parser')
table = soup.find("table")
df_ctg = pd.read_html(str(table), flavor='lxml')[0]
df_ctg.columns = df_ctg.columns.get_level_values(1)
df_ctg = df_ctg.iloc[: , 1:]
df_ctg_1 = df_ctg['Team']
df_ctg_2 = df_ctg.iloc[: , 2:7]
df_ctg = pd.concat([df_ctg_1, df_ctg_2], axis=1)

team_names = {
    'full_name':[
        'Atlanta',
        'Boston',
        'Brooklyn',
        'Charlotte',
        'Chicago',
        'Cleveland',
        'Dallas',
        'Denver',
        'Detroit',
        'Golden State',
        'Houston',
        'Indiana',
        'LA Clippers',
        'LA Lakers',
        'Memphis',
        'Miami',
        'Milwaukee',
        'Minnesota',
        'New Orleans',
        'New York',
        'Oklahoma City',
        'Orlando',
        'Philadelphia',
        'Phoenix',
        'Portland',
        'Sacramento',
        'San Antonio',
        'Toronto',
        'Utah',
        'Washington'
        ],
    'abbr_name':[
        'ATL',
        'BOS',
        'BRK',
        'CHO',
        'CHI',
        'CLE',
        'DAL',
        'DEN',
        'DET',
        'GSW',
        'HOU',
        'IND',
        'LAC',
        'LAL',
        'MEM',
        'MIA',
        'MIL',
        'MIN',
        'NOP',
        'NYK',
        'OKC',
        'ORL',
        'PHI',
        'PHO',
        'POR',
        'SAC',
        'SAS',
        'TOR',
        'UTA',
        'WAS'
        ]
}

team_name_map = pd.DataFrame(team_names)

df_ctg = pd.merge(df_ctg, team_name_map, how='inner', left_on ='Team', right_on='full_name')

del df_ctg['Team']
del df_ctg['full_name']


df_ctg = df_ctg.rename(columns={
    'abbr_name':'TEAM',
    'Exp W82':'Expected Wins (per Point Diff)',
    'W':'Wins',
    'L':'Losses'
})
df_ctg['Expected Wins (per Win%)'] = round(((df_ctg['Win%'].str.rstrip('%').astype('float') / 100.0) * 82),1)

team_tracker = pd.read_csv('support/23_24_team_tracker_template.csv')

team_tracker_results = pd.merge(team_tracker, df_ctg, how='inner', on='TEAM')

team_tracker_results_cols = [
    'TEAM',
    'Over/Under',
    'Wins',
    'Losses',
    'Win%',
    'Point Diff',
    'Expected Wins (per Win%)',
    'Expected Wins (per Point Diff)',
    'Expected O/U (per Win%)',
    'Expected O/U (per Point Diff)'
]

team_tracker_results['Expected O/U (per Win%)'] = np.where(
    team_tracker_results['Expected Wins (per Win%)'] > team_tracker_results['Over/Under'], 
    'Over',
    np.where(
        team_tracker_results['Expected Wins (per Win%)'] < team_tracker_results['Over/Under'], 'Under','Push'
    )
)

team_tracker_results['Expected O/U (per Point Diff)'] = np.where(
    team_tracker_results['Expected Wins (per Point Diff)'] > team_tracker_results['Over/Under'], 
    'Over',
    np.where(
        team_tracker_results['Expected Wins (per Point Diff)'] < team_tracker_results['Over/Under'], 'Under','Push'
    )
)

team_tracker_results = team_tracker_results[team_tracker_results_cols]
team_tracker_results.to_csv('23_24_team_tracker_results.csv')

### Tab 3 entires
ou_picks = pd.read_csv('support/23_24_ou_picks.csv')
ou_picks_for_tab = ou_picks[ou_picks['Full name'] != 'Over Under Line']

### Tab 1 Prep
ou_picks_by_team = ou_picks.copy()
del ou_picks_by_team['Moneyball: Big (3.5)']
del ou_picks_by_team['Moneyball: 1-Small (1.5)']
del ou_picks_by_team['Moneyball: 2-Small (1.5)']
del ou_picks_by_team['Moneyball: 3-Small (1.5)']

ou_picks_moneyball = ou_picks.copy()

predictions = team_tracker_results[['TEAM','Expected O/U (per Win%)','Expected O/U (per Point Diff)']]
ou_picks_moneyball = ou_picks_moneyball[['Full name','Moneyball: Big (3.5)','Moneyball: 1-Small (1.5)','Moneyball: 2-Small (1.5)','Moneyball: 3-Small (1.5)']]

team_names = {
    'Long Name':[
        'Atlanta Hawks',
        'BKN Nets',
        'Boston Celtics',
        'Charlotte Hornets',
        'Chicago Bulls',
        'Cleveland Cavaliers',
        'Dallas Mavericks',
        'Denver Nuggets',
        'Detroit Pistons',
        'GS Warriors',
        'Houston Rockets',
        'Indiana Pacers',
        'LA Clippers',
        'LA Lakers',
        'Memphis Grizzlies',
        'Miami Heat',
        'Milwaukee Bucks',
        'Minnesota Timberwolves',
        'NO Pelicans',
        'NY Knicks',
        'OKC Thunder',
        'Orlando Magic',
        'Philadelphia 76ers',
        'Phoenix Suns',
        'Portland Trail Blazers',
        'Sacramento Kings',
        'San Antonio Spurs',
        'Toronto Raptors',
        'Utah Jazz',
        'Washington Wizards'
        ],
    'TEAM':[
        'ATL',
        'BOS',
        'BRK',
        'CHO',
        'CHI',
        'CLE',
        'DAL',
        'DEN',
        'DET',
        'GSW',
        'HOU',
        'IND',
        'LAC',
        'LAL',
        'MEM',
        'MIA',
        'MIL',
        'MIN',
        'NOP',
        'NYK',
        'OKC',
        'ORL',
        'PHI',
        'PHO',
        'POR',
        'SAC',
        'SAS',
        'TOR',
        'UTA',
        'WAS'
        ]
}
team_name_map = pd.DataFrame(team_names)

predictions_rename = pd.merge(predictions,team_name_map,on='TEAM',how='inner')
del predictions_rename['TEAM']
predictions_rename = predictions_rename.rename(columns={'Long Name':'TEAM'})

ou_picks_by_team_t = ou_picks_by_team.T
ou_picks_by_team_t.columns = ou_picks_by_team_t.iloc[0]
ou_picks_by_team_t = ou_picks_by_team_t[ou_picks_by_team_t['Over Under Line'] != 'Over Under Line'].reset_index()
ou_picks_by_team_t = ou_picks_by_team_t.rename(columns={'index':'TEAM'})

prep_results_win_percent = pd.merge(ou_picks_by_team_t, predictions_rename[['TEAM','Expected O/U (per Win%)']], on='TEAM',how='inner')

entrants_col = list(prep_results_win_percent.columns)
remove_fields = ['TEAM','Over Under Line','Expected O/U (per Win%)','Expected O/U (per Point Diff)']
final_entrants = [x for x in entrants_col if x not in remove_fields]

for person in final_entrants:
    prep_results_win_percent[person] = np.where(prep_results_win_percent[person] == prep_results_win_percent['Expected O/U (per Win%)'],1,0)
prep_results_win_percent_t = prep_results_win_percent.T
prep_results_win_percent_t['Expected Correct (per Win%)'] = prep_results_win_percent_t.sum(axis=1)
prep_results_win_percent_t = prep_results_win_percent_t.reset_index()
prep_results_win_percent_t = prep_results_win_percent_t.rename(columns={'index':'Name'})
prep_results_win_percent_t = prep_results_win_percent_t[~prep_results_win_percent_t['Name'].isin(['TEAM','Over Under Line','Expected O/U (per Win%)'])]
prep_results_win_percent_t = prep_results_win_percent_t[['Name','Expected Correct (per Win%)']].reset_index(drop=True)

prep_results_point_diff = pd.merge(ou_picks_by_team_t, predictions_rename[['TEAM','Expected O/U (per Point Diff)']], on='TEAM',how='inner')
for person in final_entrants:
    prep_results_point_diff[person] = np.where(prep_results_point_diff[person] == prep_results_point_diff['Expected O/U (per Point Diff)'],1,0)
prep_results_point_diff_t = prep_results_point_diff.T
prep_results_point_diff_t['Expected Correct (per Point Diff)'] = prep_results_point_diff_t.sum(axis=1)
prep_results_point_diff_t = prep_results_point_diff_t.reset_index()
prep_results_point_diff_t = prep_results_point_diff_t.rename(columns={'index':'Name'})
prep_results_point_diff_t = prep_results_point_diff_t[~prep_results_point_diff_t['Name'].isin(['TEAM','Over Under Line','Expected O/U (per Point Diff)'])]
prep_results_point_diff_t = prep_results_point_diff_t[['Name','Expected Correct (per Point Diff)']].reset_index(drop=True)      

prep_results_winner = pd.merge(prep_results_win_percent_t, prep_results_point_diff_t, on='Name',how='inner')

ou_picks_moneyball_prep = ou_picks_moneyball[ou_picks_moneyball['Full name'] != 'Over Under Line']

def moneyball_predictions(moneyball_column, winp_or_pointdiff, output_column, big_or_small):
    ou_picks_moneyball_prep_df = ou_picks_moneyball_prep[['Full name',moneyball_column]]

    # ugly iteration but it will work for now
    moneyball_df = pd.DataFrame()
    for name in ou_picks_moneyball_prep_df['Full name']:
        selected_team = ou_picks_moneyball_prep_df[ou_picks_moneyball_prep_df['Full name']==name][moneyball_column].reset_index(drop=True).iloc[0]
        ou_pick = ou_picks_by_team[ou_picks_by_team['Full name'] == name][selected_team].reset_index(drop=True).iloc[0]
        df_row = pd.DataFrame({'Full name':name,'TEAM':selected_team,'Pick':ou_pick},index=[0])
        moneyball_df = pd.concat([moneyball_df,df_row])

    moneyball_df = pd.merge(moneyball_df,predictions_rename[['TEAM',winp_or_pointdiff]],on='TEAM',how='inner')
    
    if big_or_small == 'Big':
        moneyball_df[output_column] = np.where(moneyball_df['Pick'] == moneyball_df[winp_or_pointdiff],3.5,0)
    elif big_or_small == 'Small':
        moneyball_df[output_column] = np.where(moneyball_df['Pick'] == moneyball_df[winp_or_pointdiff],1.5,0)
    else:
        print('big v small error')

    moneyball_df = moneyball_df[['Full name',output_column]]
    
    return moneyball_df

# Moneyball: Win %
moneyball_big_df_winp = moneyball_predictions('Moneyball: Big (3.5)', 'Expected O/U (per Point Diff)','Moneyball - Big: Point Diff','Big')
moneyball_small1_df_winp = moneyball_predictions('Moneyball: 1-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small1: Point Diff','Small')
moneyball_small2_df_winp = moneyball_predictions('Moneyball: 2-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small2: Point Diff','Small')
moneyball_small3_df_winp = moneyball_predictions('Moneyball: 3-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small3: Point Diff','Small')

# Moneyball: Point Diff
moneyball_big_df_point_diff = moneyball_predictions('Moneyball: Big (3.5)', 'Expected O/U (per Point Diff)','Moneyball - Big: Point Diff','Big')
moneyball_small1_df_point_diff = moneyball_predictions('Moneyball: 1-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small1: Point Diff','Small')
moneyball_small2_df_point_diff = moneyball_predictions('Moneyball: 2-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small2: Point Diff','Small')
moneyball_small3_df_point_diff = moneyball_predictions('Moneyball: 3-Small (1.5)', 'Expected O/U (per Point Diff)','Moneyball - Small3: Point Diff','Small')

mb_winp_final = pd.merge(moneyball_big_df_winp, moneyball_small1_df_winp, on='Full name',how='inner')
mb_winp_final = pd.merge(mb_winp_final, moneyball_small2_df_winp, on='Full name',how='inner')
mb_winp_final = pd.merge(mb_winp_final, moneyball_small3_df_winp, on='Full name',how='inner')
# mb_winp_final['Expected Bonus Points (per Win%)'] = point_diff_mb_final.sum(axis=1)
mb_winp_final.index = mb_winp_final['Full name']
del mb_winp_final['Full name']
mb_winp_final['Expected Bonus Points (per Win%)'] = mb_winp_final.sum(axis=1)
mb_winp_final = mb_winp_final.reset_index()
mb_winp_final = mb_winp_final[['Full name','Expected Bonus Points (per Win%)']]
mb_winp_final = mb_winp_final.rename(columns={'Full name':'Name'})

mb_pointdiff_final = pd.merge(moneyball_big_df_point_diff, moneyball_small1_df_point_diff, on='Full name',how='inner')
mb_pointdiff_final = pd.merge(mb_pointdiff_final, moneyball_small2_df_point_diff, on='Full name',how='inner')
mb_pointdiff_final = pd.merge(mb_pointdiff_final, moneyball_small3_df_point_diff, on='Full name',how='inner')
mb_pointdiff_final.index = mb_pointdiff_final['Full name']
del mb_pointdiff_final['Full name']
mb_pointdiff_final['Expected Bonus Points (per Point Diff)'] = mb_pointdiff_final.sum(axis=1)
mb_pointdiff_final = mb_pointdiff_final.reset_index()
mb_pointdiff_final = mb_pointdiff_final[['Full name','Expected Bonus Points (per Point Diff)']]
mb_pointdiff_final = mb_pointdiff_final.rename(columns={'Full name':'Name'})

## Expected Correct
prep_results_winner_final = pd.merge(prep_results_winner,mb_winp_final,on='Name',how='inner')
prep_results_winner_final = pd.merge(prep_results_winner_final,mb_pointdiff_final,on='Name',how='inner')
prep_results_winner_final['Expected Total Score (Per Win%)'] = prep_results_winner_final['Expected Correct (per Win%)'] + prep_results_winner_final['Expected Bonus Points (per Win%)']
prep_results_winner_final['Expected Total Score (Per Point Diff)'] = prep_results_winner_final['Expected Correct (per Point Diff)'] + prep_results_winner_final['Expected Bonus Points (per Point Diff)']

prep_results_winner_final = prep_results_winner_final.sort_values('Expected Total Score (Per Point Diff)',ascending=False).reset_index(drop=True)
prep_results_winner_final = prep_results_winner_final.reset_index().rename(columns={'index':'Expected Result (per Point Diff)'})
prep_results_winner_final['Expected Result (per Point Diff)'] = prep_results_winner_final['Expected Result (per Point Diff)'] + 1

prep_results_winner_final = prep_results_winner_final.sort_values('Expected Total Score (Per Win%)',ascending=False).reset_index(drop=True)
prep_results_winner_final = prep_results_winner_final.reset_index().rename(columns={'index':'Expected Result (per Win%)'})
prep_results_winner_final['Expected Result (per Win%)'] = prep_results_winner_final['Expected Result (per Win%)'] + 1

result_col_order = [
    'Name',
    'Expected Correct (per Win%)',
    'Expected Correct (per Point Diff)',
    'Expected Bonus Points (per Win%)',
    'Expected Bonus Points (per Point Diff)',
    'Expected Total Score (Per Win%)',
    'Expected Total Score (Per Point Diff)',
    'Expected Result (per Win%)',
    'Expected Result (per Point Diff)',
]

prep_results_winner_final = prep_results_winner_final[result_col_order]

## Final Outputs ##

# Tab 1
print(prep_results_winner_final)
prep_results_winner_final.to_csv('outputs/tab1_prep_results_winner_final.csv',index=False)

# Tab 2
print(team_tracker_results)
team_tracker_results.to_csv('outputs/tab2_team_tracker_results.csv',index=False)

# Tab 3
print(ou_picks_for_tab)
ou_picks_for_tab.to_csv('outputs/tab3_ou_picks_for_tab.csv',index=False)

