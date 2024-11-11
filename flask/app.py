# https://15xu0h4j6i.execute-api.us-east-2.amazonaws.com/dev

from flask import Flask, render_template
from flask import Flask, request, url_for

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True

import boto3
from datetime import datetime
from datetime import date
from io import StringIO
import math
import numpy as np
import os
import pandas as pd
import sys
import gviz_api
import json

## Functions ##
def data_ingest(file_name='static/22_23_wp_final_results.csv'):
	df = pd.read_csv(file_name)
	# df['TEAM'] = df['TEAM'].str.title()
	
	return df

def aws_ingest(filename='22_23_wp_final_results.csv'): 
	aws_id = os.getenv("aws_id")
	aws_secret = os.getenv("aws_secret")
	client = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)

	bucket_name = 'nba-ou'

	csv_obj = client.get_object(Bucket=bucket_name, Key=filename)
	body = csv_obj['Body']
	csv_string = body.read().decode('utf-8')

	df = pd.read_csv(StringIO(csv_string))

	return df


## Main Process ##
# Flask application
def main(filename='22_23_wp_final_results.csv'):

	# aws process
	df_final = aws_ingest(filename)

	# local process
	# df_final = data_ingest(filename)

	return df_final

## Main Execution ##
@app.route('/', methods=['GET','POST'])
def index():
	df = main('tab1_prep_results_winner_final.csv')

	table_description = {"Name": ("string", "Name"),
						"Expected Competition Result (per Win%)": ("number", "Ranking (WP)"),
						"Expected Competition Result (per Point Diff)": ("number", "Ranking (PD)"),
						"Expected Correct (per Win%)": ("number", "Predictions (WP)"),
						"Expected Correct (per Point Diff)": ("number", "Predictions (PD)"),
						"Expected Bonus Points (per Win%)": ("number", "Bonus Points (WP)"),
						"Expected Bonus Points (per Point Diff)": ("number", "Bonus Points (PD)"),
						"Expected Total Score (Per Win%)": ("number", "Total Points (WP)"),
						"Expected Total Score (Per Point Diff)": ("number", "Total Points (PD)"),
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("Name","Expected Competition Result (per Win%)","Expected Competition Result (per Point Diff)","Expected Correct (per Win%)","Expected Correct (per Point Diff)","Expected Bonus Points (per Win%)","Expected Bonus Points (per Point Diff)","Expected Total Score (Per Win%)","Expected Total Score (Per Point Diff)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('ou_table.html',  table=json_table, context=context)


@app.route('/23_24', methods=['GET', 'POST'])
def index_23_24():
	df = main('23_24_tab1_prep_results_winner_final.csv')

	table_description = {"Name": ("string", "Name"),
						 "Expected Competition Result (per Win%)": ("number", "Ranking (WP)"),
						 "Expected Competition Result (per Point Diff)": ("number", "Ranking (PD)"),
						 "Expected Correct (per Win%)": ("number", "Predictions (WP)"),
						 "Expected Correct (per Point Diff)": ("number", "Predictions (PD)"),
						 "Expected Bonus Points (per Win%)": ("number", "Bonus Points (WP)"),
						 "Expected Bonus Points (per Point Diff)": ("number", "Bonus Points (PD)"),
						 "Expected Total Score (Per Win%)": ("number", "Total Points (WP)"),
						 "Expected Total Score (Per Point Diff)": ("number", "Total Points (PD)"),
						 }

	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=(
	"Name", "Expected Competition Result (per Win%)", "Expected Competition Result (per Point Diff)",
	"Expected Correct (per Win%)", "Expected Correct (per Point Diff)", "Expected Bonus Points (per Win%)",
	"Expected Bonus Points (per Point Diff)", "Expected Total Score (Per Win%)",
	"Expected Total Score (Per Point Diff)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('23_24_ou_table.html', table=json_table, context=context)

@app.route('/team_tracking', methods=['GET','POST'])
def index_team_table():
	df = main('tab2_team_tracker_results.csv')
	# sort = request.args.get('sort', 'Team_Name')
	# reverse = (request.args.get('direction', 'asc') == 'desc')
	# df = df.sort_values(by=[sort], ascending=reverse)

	table_description = {"TEAM": ("string", "Team"),
						"OU Wins": ("number", "Over/Under client"),
						"Wins": ("number", "Wins"),
						"Losses": ("number", "Losses"),
						"Win%": ("number", "Win%"),
						"Point Diff": ("number", "Point Diff"),
						"Expected Wins (per Win%)": ("number", "Expected Wins (WP)"),
						"Expected Wins (per Point Diff)": ("number", "Expected Wins (PD)"),
						"Expected O/U (per Win%)": ("string", "Expected O/U (WP)"),
						"Expected O/U (per Point Diff)": ("string", "Expected O/U (PD)"),
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("TEAM","OU Wins","Wins","Losses","Win%","Point Diff","Expected Wins (per Win%)","Expected Wins (per Point Diff)","Expected O/U (per Win%)","Expected O/U (per Point Diff)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('team_table.html',  table=json_table, context=context)


@app.route('/team_tracking_23_24', methods=['GET', 'POST'])
def index_team_table_23_24():
	df = main('23_24_tab2_team_tracker_results.csv')
	# sort = request.args.get('sort', 'Team_Name')
	# reverse = (request.args.get('direction', 'asc') == 'desc')
	# df = df.sort_values(by=[sort], ascending=reverse)

	table_description = {"TEAM": ("string", "Team"),
						 "OU Wins": ("number", "Over/Under client"),
						 "Wins": ("number", "Wins"),
						 "Losses": ("number", "Losses"),
						 "Win%": ("number", "Win%"),
						 "Point Diff": ("number", "Point Diff"),
						 "Expected Wins (per Win%)": ("number", "Expected Wins (WP)"),
						 "Expected Wins (per Point Diff)": ("number", "Expected Wins (PD)"),
						 "Expected O/U (per Win%)": ("string", "Expected O/U (WP)"),
						 "Expected O/U (per Point Diff)": ("string", "Expected O/U (PD)"),
						 }

	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=(
	"TEAM", "OU Wins", "Wins", "Losses", "Win%", "Point Diff", "Expected Wins (per Win%)",
	"Expected Wins (per Point Diff)", "Expected O/U (per Win%)", "Expected O/U (per Point Diff)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('23_24_team_table.html', table=json_table, context=context)


@app.route('/entries', methods=['GET','POST'])
def index_entry_table():
	df = main('tab3_ou_picks_for_tab.csv')
	# sort = request.args.get('sort', 'Team_Name')
	# reverse = (request.args.get('direction', 'asc') == 'desc')
	# df = df.sort_values(by=[sort], ascending=reverse)

	table_description = {"Full name": ("string", "Name"),
						"Boston Celtics": ("string","Boston Celtics"),
						"Milwaukee Bucks": ("string","Milwaukee Bucks"),
						"Denver Nuggets": ("string","Denver Nuggets"),
						"Phoenix Suns": ("string","Phoenix Suns"),
						"Cleveland Cavaliers": ("string","Cleveland Cavaliers"),
						"Philadelphia 76ers": ("string","Philadelphia 76ers"),
						"GS Warriors": ("string","GS Warriors"),
						"LA Lakers": ("string","LA Lakers"),
						"LA Clippers": ("string","LA Clippers"),
						"Memphis Grizzlies": ("string","Memphis Grizzlies"),
						"Miami Heat": ("string","Miami Heat"),
						"NY Knicks": ("string","NY Knicks"),
						"Minnesota Timberwolves": ("string","Minnesota Timberwolves"),
						"NO Pelicans": ("string","NO Pelicans"),
						"Sacramento Kings": ("string","Sacramento Kings"),
						"OKC Thunder": ("string","OKC Thunder"),
						"Dallas Mavericks": ("string","Dallas Mavericks"),
						"Atlanta Hawks": ("string","Atlanta Hawks"),
						"Indiana Pacers": ("string","Indiana Pacers"),
						"BKN Nets": ("string","BKN Nets"),
						"Chicago Bulls": ("string","Chicago Bulls"),
						"Orlando Magic": ("string","Orlando Magic"),
						"Toronto Raptors": ("string","Toronto Raptors"),
						"Utah Jazz": ("string","Utah Jazz"),
						"Houston Rockets": ("string","Houston Rockets"),
						"Charlotte Hornets": ("string","Charlotte Hornets"),
						"San Antonio Spurs": ("string","San Antonio Spurs"),
						"Portland Trail Blazers": ("string","Portland Trail Blazers"),
						"Detroit Pistons": ("string","Detroit Pistons"),
						"Washington Wizards": ("string","Washington Wizards"),
						"Moneyball: Big (3.5)": ("string","Moneyball: Big (3.5)"),
						"Moneyball: 1-Small (1.5)": ("string","Moneyball: 1-Small (1.5)"),
						"Moneyball: 2-Small (1.5)": ("string","Moneyball: 2-Small (1.5)"),
						"Moneyball: 3-Small (1.5)": ("string","Moneyball: 3-Small (1.5)"),
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("Full name","Boston Celtics","Milwaukee Bucks","Denver Nuggets","Phoenix Suns","Cleveland Cavaliers","Philadelphia 76ers","GS Warriors","LA Lakers","LA Clippers","Memphis Grizzlies","Miami Heat","NY Knicks","Minnesota Timberwolves","NO Pelicans","Sacramento Kings","OKC Thunder","Dallas Mavericks","Atlanta Hawks","Indiana Pacers","BKN Nets","Chicago Bulls","Orlando Magic","Toronto Raptors","Utah Jazz","Houston Rockets","Charlotte Hornets","San Antonio Spurs","Portland Trail Blazers","Detroit Pistons","Washington Wizards","Moneyball: Big (3.5)","Moneyball: 1-Small (1.5)","Moneyball: 2-Small (1.5)","Moneyball: 3-Small (1.5)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('entry_table.html',  table=json_table, context=context)


@app.route('/entries_23_24', methods=['GET', 'POST'])
def index_entry_table_23_24():
	df = main('23_24_tab3_ou_picks_for_tab.csv')
	# sort = request.args.get('sort', 'Team_Name')
	# reverse = (request.args.get('direction', 'asc') == 'desc')
	# df = df.sort_values(by=[sort], ascending=reverse)

	table_description = {"Full name": ("string", "Name"),
						 "Boston Celtics": ("string", "Boston Celtics"),
						 "Milwaukee Bucks": ("string", "Milwaukee Bucks"),
						 "Denver Nuggets": ("string", "Denver Nuggets"),
						 "Phoenix Suns": ("string", "Phoenix Suns"),
						 "Cleveland Cavaliers": ("string", "Cleveland Cavaliers"),
						 "Philadelphia 76ers": ("string", "Philadelphia 76ers"),
						 "GS Warriors": ("string", "GS Warriors"),
						 "LA Lakers": ("string", "LA Lakers"),
						 "LA Clippers": ("string", "LA Clippers"),
						 "Memphis Grizzlies": ("string", "Memphis Grizzlies"),
						 "Miami Heat": ("string", "Miami Heat"),
						 "NY Knicks": ("string", "NY Knicks"),
						 "Minnesota Timberwolves": ("string", "Minnesota Timberwolves"),
						 "NO Pelicans": ("string", "NO Pelicans"),
						 "Sacramento Kings": ("string", "Sacramento Kings"),
						 "OKC Thunder": ("string", "OKC Thunder"),
						 "Dallas Mavericks": ("string", "Dallas Mavericks"),
						 "Atlanta Hawks": ("string", "Atlanta Hawks"),
						 "Indiana Pacers": ("string", "Indiana Pacers"),
						 "BKN Nets": ("string", "BKN Nets"),
						 "Chicago Bulls": ("string", "Chicago Bulls"),
						 "Orlando Magic": ("string", "Orlando Magic"),
						 "Toronto Raptors": ("string", "Toronto Raptors"),
						 "Utah Jazz": ("string", "Utah Jazz"),
						 "Houston Rockets": ("string", "Houston Rockets"),
						 "Charlotte Hornets": ("string", "Charlotte Hornets"),
						 "San Antonio Spurs": ("string", "San Antonio Spurs"),
						 "Portland Trail Blazers": ("string", "Portland Trail Blazers"),
						 "Detroit Pistons": ("string", "Detroit Pistons"),
						 "Washington Wizards": ("string", "Washington Wizards"),
						 "Moneyball: Big (3.5)": ("string", "Moneyball: Big (3.5)"),
						 "Moneyball: 1-Small (1.5)": ("string", "Moneyball: 1-Small (1.5)"),
						 "Moneyball: 2-Small (1.5)": ("string", "Moneyball: 2-Small (1.5)"),
						 "Moneyball: 3-Small (1.5)": ("string", "Moneyball: 3-Small (1.5)"),
						 }

	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=(
	"Full name", "Boston Celtics", "Milwaukee Bucks", "Denver Nuggets", "Phoenix Suns", "Cleveland Cavaliers",
	"Philadelphia 76ers", "GS Warriors", "LA Lakers", "LA Clippers", "Memphis Grizzlies", "Miami Heat", "NY Knicks",
	"Minnesota Timberwolves", "NO Pelicans", "Sacramento Kings", "OKC Thunder", "Dallas Mavericks", "Atlanta Hawks",
	"Indiana Pacers", "BKN Nets", "Chicago Bulls", "Orlando Magic", "Toronto Raptors", "Utah Jazz", "Houston Rockets",
	"Charlotte Hornets", "San Antonio Spurs", "Portland Trail Blazers", "Detroit Pistons", "Washington Wizards",
	"Moneyball: Big (3.5)", "Moneyball: 1-Small (1.5)", "Moneyball: 2-Small (1.5)", "Moneyball: 3-Small (1.5)"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('23_24_entry_table.html', table=json_table, context=context)


if __name__ == "__main__":
	app.run(debug=True)




































