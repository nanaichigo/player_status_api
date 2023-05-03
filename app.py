import os
import sys

from datetime import datetime, date, timedelta
import json 

from flask import Flask, jsonify, request, abort
from flask.json import JSONEncoder
from myexception import MyException, InputException, ServerException

import sys 
import traceback
import json 
import configparser
#from flask_cors import CORS

from DBAccess import DBAccess

sys.path.insert(0, os.path.dirname(__file__))

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
#CORS(app)


config_ini = configparser.ConfigParser()
config_ini.read('config.ini', encoding='utf-8')

DEFAULT = "DEFAULT"
RUGBY = "Rugby"
HOST = config_ini[DEFAULT]["Host"]
USER = config_ini[DEFAULT]["User"]
PASSWD = config_ini[DEFAULT]["Password"]
RUGBY_DBNAME = config_ini[RUGBY]["DBNAME"]



@app.errorhandler(MyException)
def error_my_except(e):
    return jsonify({'message': e.message}), e.code

class JsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.strftime("%Y/%m/%d")
        elif isinstance(obj, datetime):
            return obj.strftime("%Y/%m/%d %H:%M:%S")
        elif isinstance(obj, timedelta):
            return str(obj)
        return JSONEncoder.default(self, obj)

app.json_encoder = JsonEncoder


@app.route('/official')
def get_ranking():
    try:
        player_SQL = "select name_id, player.name as name, played, score, try, goal, pg, dg, goal_attempt, \
            pg_attempt, dg_attempt, g_p, pg_p, dg_p from (select player.name_group_id as name_id, \
        count(game.game_no) as played, sum(score) as score, sum(try) as try ,sum(goal) as goal ,sum(dg) as dg, sum(pg) as pg,\
        sum(goal_attempt) as goal_attempt, sum(pg_attempt) as pg_attempt, sum(dg_attempt) as dg_attempt,\
        sum(goal)/sum(goal_attempt) as g_p, sum(pg)/sum(pg_attempt) as pg_p, sum(dg)/sum(dg_attempt) as dg_p\
        from player_status, player, game, tournament, team\
        where player.id = player_status.player_id and game.game_no = player_status.game_no \
        and game.convention_name = tournament.tournament_id and team.team_id = player_status.team_id \
        and tournament.official = 1\
        and player_status.played = 1 group by player.name_group_id) as a, player \
        where a.name_id = player.id"
        
        req = request.args
        r_type = req.get("r_type")
        type_query = "played"   
        if r_type is not None:
            if r_type == 'score' or r_type == 'try' or r_type == "goal" or r_type == "pg" or  r_type == "dg":
                type_query = r_type
        
        min_target = req.get("min_target", 0, type=int)
        max_target = req.get("max_target", 0, type=int)
        
        range = ""
        if min_target > 0:
            if min_target == max_target:
                range = f"and {type_query} = {min_target}"
            elif max_target > min_target:
                range = f"and {type_query} between {min_target} and {max_target}"
            else:
                range = f"and {type_query} between {max_target} and {min_target}"
        elif max_target > 0:
            range = f"and {type_query} <= {max_target}"
            
        query = f"{player_SQL} {range} order by {type_query} desc"
        
        with DBAccess(HOST, USER, PASSWD, RUGBY_DBNAME) as access:
            data = access.getList(query)
 
        response = {}
        
        response["body"] = {
            "data": data
        }
        response["status"] = 200
        
        return jsonify(response)
    except Exception as e:
        t, v, tb = sys.exc_info()
        print(traceback.format_exception(t,v,tb))
        print(traceback.format_tb(e.__traceback__))
        raise ServerException('server exception')

@app.route('/custom')
def get_custom_ranking():
    try:
        req = request.args
        r_type = req.get("r_type")
        type_query = "played"   
        if r_type is not None:
            if r_type == 'score' or r_type == 'try' or r_type == "goal" or r_type == "pg" or  r_type == "dg":
                type_query = r_type
                
        min_target = req.get("min_target", 0, type=int)
        max_target = req.get("max_target", 0, type=int)
               
        range = ""
        if min_target > 0:
            if min_target == max_target:
                range = f"and {type_query} = {min_target}"
            elif max_target > min_target:
                range = f"and {type_query} between {min_target} and {max_target}"
            else:
                range = f"and {type_query} between {max_target} and {min_target}"
        elif max_target > 0:
            range = f"and {type_query} <= {max_target}"
        
        tournament_q = []
        
        def is_nth_bit_set(num: int, n:int):
            if num & (1 << (n-1)):
                return True
            return False
        
        tournament_params = req.get("tournamentParams", type=int)
        
        tournament_query = []
        if(is_nth_bit_set(tournament_params, 1)):
            # leagueone
            leagueone_params = req.get("leagueParams", type=int)
            leagueone_division = req.get("leagueDiv", type=int)
            leagueone_divs = []
            if(is_nth_bit_set(leagueone_division, 1)):
                leagueone_divs.append("division LIKE '%Div1%'")
            if(is_nth_bit_set(leagueone_division, 2)):
                leagueone_divs.append("division LIKE '%Div2%'")
            if(is_nth_bit_set(leagueone_division, 3)):
                leagueone_divs.append("division LIKE '%Div3%'")
            divs_text = "({})".format(" or ".join(leagueone_divs))
            
            leagueone_tournament = []
            if(is_nth_bit_set(leagueone_params, 1)):
                leagueone_tournament.append("regular = 1")
            if(is_nth_bit_set(leagueone_params, 2)):
                leagueone_tournament.append("playoff = 1")
            if(is_nth_bit_set(leagueone_params, 3)):
                leagueone_tournament.append("is_changedivision = 1")
            tournament_text = "({})".format(" or ".join(leagueone_tournament))
            
            tournament_query.append("(" + divs_text + " and " + tournament_text + ")")
            
        if(is_nth_bit_set(tournament_params, 2)):
            # topleague
            topleague_params = req.get("topParams", type=int)
            topleague_division = req.get("topDiv", type=int)
            topleague_divs = []
            topleague_tournament = []
            
            if(is_nth_bit_set(topleague_division, 1)):
                topleague_divs.append("division LIKE '%TL%'")
            if(is_nth_bit_set(topleague_division, 2)):
                topleague_divs.append("division LIKE '%TLC%' or division LIKE '%TCL%'")
                
            divs_text = "({})".format(" or ".join(topleague_divs))
            
            if(is_nth_bit_set(topleague_params, 1)):
                topleague_tournament.append("regular = 1")
            if(is_nth_bit_set(topleague_params, 2)):
                topleague_tournament.append("playoff = 1")
            if(is_nth_bit_set(topleague_params, 3)):
                topleague_tournament.append("is_changedivision = 1")
            tournament_text = "({})".format(" or ".join(topleague_tournament))
            
            tournament_query.append("(" + divs_text + " and " + tournament_text + ")")
            
        if(is_nth_bit_set(tournament_params, 3)):
            # cup
            divs_text = "division = 'TL'"
            cup_params = req.get("cupParams", type=int)
            cup_tournament = []
            if(is_nth_bit_set(cup_params, 1)):
                cup_tournament.append("regular = 1")
            if(is_nth_bit_set(cup_params, 2)):
                cup_tournament.append("playoff = 1")
                
            tournament_text = "({})".format(" or ".join(cup_tournament))
            tournament_query.append("(" + divs_text + " and " + tournament_text + ")")
            
        if(is_nth_bit_set(tournament_params, 4)):
            # preseason
            divs_text = "division = 'TL'"
            pre_params = req.get("preParams", type=int)
            pre_tournament = []
            if(is_nth_bit_set(pre_params, 1)):
                pre_tournament.append("regular = 1")
            if(is_nth_bit_set(pre_params, 2)):
                pre_tournament.append("playoff = 1")
            tournament_text = "({})".format(" or ".join(pre_tournament))
            tournament_query.append("(" + divs_text + " and " + tournament_text + ")")
        
        if len(tournament_query) > 1:
            tournament_all_query = "(" + " or ".join(tournament_query) + ")"
        else:
            tournament_all_query = tournament_query[0]
            
        query = f"select name_id, player.name as name, played, score, try, goal, pg, dg, goal_attempt, \
            pg_attempt, dg_attempt, g_p, pg_p, dg_p from (select player.name_group_id as name_id, \
        count(game.game_no) as played, sum(score) as score, sum(try) as try ,sum(goal) as goal ,sum(dg) as dg, sum(pg) as pg,\
        sum(goal_attempt) as goal_attempt, sum(pg_attempt) as pg_attempt, sum(dg_attempt) as dg_attempt,\
        sum(goal)/sum(goal_attempt) as g_p, sum(pg)/sum(pg_attempt) as pg_p, sum(dg)/sum(dg_attempt) as dg_p\
        from player_status, player, game, tournament, team\
        where player.id = player_status.player_id and game.game_no = player_status.game_no \
        and game.convention_name = tournament.tournament_id and team.team_id = player_status.team_id \
        and {tournament_all_query} \
        and player_status.played = 1 group by player.name_group_id) as a, player \
        where a.name_id = player.id {range} order by {type_query} desc"
        
        with DBAccess(HOST, USER, PASSWD, RUGBY_DBNAME) as access:
            data = access.getList(query)
        
        response = {}
        
        response["body"] = {
            "data": data
        }
        response["status"] = 200
        
        return jsonify(response)
    except Exception as e:
        t, v, tb = sys.exc_info()
        print(traceback.format_exception(t,v,tb))
        print(traceback.format_tb(e.__traceback__))
        raise ServerException('server exception')
    
@app.route('/player')
def get_player():
    try:
        req = request.args

        player_id = req.get("player_id", 0, type=int)
            
        query = f"select season.season_name,\
                tournament.name_abbreviation as tournament, tournament.division, tournament.section, \
                tournament.regular, tournament.playoff, \
                tournament.is_cup, tournament.is_underdivision , tournament.is_changedivision, \
                tournament.is_other, tournament.is_preseason,\
                team.team_abbreviation as team, player.name, \
                date, played, score, try, goal, pg, dg, goal_attempt, pg_attempt, dg_attempt  \
                from player_status, game, tournament, team, player, season\
                where game.game_no = player_status.game_no and tournament.tournament_id = game.convention_name \
                and player_status.team_id = team.team_id \
                and player_id in (select id from player where player.name_group_id = {player_id})\
                and player.id =player_status.player_id \
                and tournament.season_id  = season.season_id order by date asc"

        with DBAccess(HOST, USER, PASSWD, RUGBY_DBNAME) as access:
            data = access.getList(query)
        
        profile = {}
        profile["first_played"] = {"date": data[0]["date"], "team":data[0]["team"]}
        profile["last_played"] = {"date": data[-1]["date"], "team":data[-1]["team"]}
        profile["team"] = []
        profile["name"] = []
        tmp_team = data[0]["team"]
        tmp_team_season = data[0]["season_name"]
        
        tmp_name = data[0]["name"]
        tmp_name_season = data[0]["season_name"]
        
        caps_league = {}
        caps_po = {}
        caps_cup = {}
        caps_change = {}
        caps_under = {}
        caps_preseason = {}
        caps_other = {}
        
        INITIALIZE_DICT_KEY = ["played","score","try","goal","pg","dg"]
        
        for idx, d in enumerate(data):
            if(d["regular"] == 1 or d["playoff"] == 1 or d["is_cup"] == 1 or d["is_changedivision"] == 1 or d["is_underdivision"] == 1):
                if tmp_team != d["team"]:
                    if tmp_team_season == d["season_name"]:
                        profile["team"].append(f"{tmp_team}({tmp_team_season})")
                    else:
                        profile["team"].append(f"{tmp_team}({tmp_team_season}～{data[idx-1]['season_name']})")
                    tmp_team = d["team"]
                    tmp_team_season = d["season_name"]
                    
                if tmp_name != d["name"]:
                    if tmp_name_season == d["season_name"]:
                        profile["name"].append(f"{tmp_name}({tmp_name_season})")
                    else:
                        profile["name"].append(f"{tmp_name}({tmp_name_season}～{data[idx-1]['season_name']})")
                    tmp_name = d["name"]
                    tmp_name_season = d["season_name"]
                    
            division = d["division"]
            if d["regular"] == 1:
                if division not in caps_league:
                    caps_league[division] = {key: 0 for key in INITIALIZE_DICT_KEY}
                                        
                if d["played"] == 1:
                    caps_league[division]["played"] += 1
                    caps_league[division]["score"] += d["score"]
                    caps_league[division]["try"] += d["try"]
                    caps_league[division]["goal"] += d["goal"]
                    caps_league[division]["pg"] += d["pg"]
                    caps_league[division]["dg"] += d["dg"]
                
            if d["playoff"] == 1:
                if division not in caps_po:
                    caps_po[division] = {key:0 for key in INITIALIZE_DICT_KEY}
                    
                if d["played"] == 1:
                    caps_po[division]["played"] += 1
                    caps_po[division]["score"] += d["score"]
                    caps_po[division]["try"] += d["try"]
                    caps_po[division]["goal"] += d["goal"]
                    caps_po[division]["pg"] += d["pg"]
                    caps_po[division]["dg"] += d["dg"]
                
            if d["is_cup"] == 1:
                if division not in caps_cup:
                    caps_cup[division] = {key:0 for key in INITIALIZE_DICT_KEY}
                    
                if d["played"] == 1:
                    caps_cup[division]["played"] += 1
                    caps_cup[division]["score"] += d["score"]
                    caps_cup[division]["try"] += d["try"]
                    caps_cup[division]["goal"] += d["goal"]
                    caps_cup[division]["pg"] += d["pg"]
                    caps_cup[division]["dg"] += d["dg"]
                
            if d["is_changedivision"] == 1:
                if division not in caps_change:
                    caps_change[division] = {key:0 for key in INITIALIZE_DICT_KEY}
                    
                if d["played"] == 1:
                    caps_change[division]["played"] += 1
                    caps_change[division]["score"] += d["score"]
                    caps_change[division]["try"] += d["try"]
                    caps_change[division]["goal"] += d["goal"]
                    caps_change[division]["pg"] += d["pg"]
                    caps_change[division]["dg"] += d["dg"]
            
            if d["is_underdivision"] == 1:
                if division not in caps_under:
                    caps_under[division] = {key:0 for key in INITIALIZE_DICT_KEY}
                    
                if d["played"] == 1:
                    caps_under[division]["played"] += 1
                    caps_under[division]["score"] += d["score"]
                    caps_under[division]["try"] += d["try"]
                    caps_under[division]["goal"] += d["goal"]
                    caps_under[division]["pg"] += d["pg"]
                    caps_under[division]["dg"] += d["dg"]
                    
            if d["is_preseason"] == 1:
                if division not in caps_preseason:
                    caps_preseason[division] = {key:0 for key in INITIALIZE_DICT_KEY}
                      
                if d["played"] == 1:
                    caps_preseason[division]["played"] += 1
                    caps_preseason[division]["score"] += d["score"]
                    caps_preseason[division]["try"] += d["try"]
                    caps_preseason[division]["goal"] += d["goal"]
                    caps_preseason[division]["pg"] += d["pg"]
                    caps_preseason[division]["dg"] += d["dg"]
                    
            if d["is_other"] == 1:
                if division not in caps_other:
                    caps_other[division] = {key:0 for key in INITIALIZE_DICT_KEY}
 
                if d["played"] == 1:
                    caps_other[division]["played"] += 1
                    caps_other[division]["score"] += d["score"]
                    caps_other[division]["try"] += d["try"]
                    caps_other[division]["goal"] += d["goal"]
                    caps_other[division]["pg"] += d["pg"]
                    caps_other[division]["dg"] += d["dg"]
                     
        if tmp_team_season == data[-1]["season_name"]:
            profile["team"].append(f"{tmp_team}({tmp_team_season})")
        else:
            profile["team"].append(f"{tmp_team}({tmp_team_season}～{data[idx-1]['season_name']})")  
            
        if tmp_name_season == data[-1]["season_name"]:
            profile["name"].append(f"{tmp_name}({tmp_name_season})")
        else:
            profile["name"].append(f"{tmp_name}({tmp_name_season}～{data[idx-1]['season_name']})")     
        
        response = {}
        
        response["body"] = {
            "data": data,
            "caps_types":{
                "league": caps_league,
                "po": caps_po,
                "cup": caps_cup,
                "change": caps_change,
                "under": caps_under,
                "preseason": caps_preseason,
                "other":caps_other,
            },
            "profile": profile,
        }        
        
        
        response["status"] = 200
        
        return jsonify(response)
    except Exception as e:
        t, v, tb = sys.exc_info()
        print(traceback.format_exception(t,v,tb))
        print(traceback.format_tb(e.__traceback__))
        raise ServerException('server exception')