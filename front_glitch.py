import requests
import json
from bottle import route, run, template, error

@route('/')
def index():
    return('<h1> welcome to glitch, please choose you a database </h1>')
    #return()

@error(404)
def error404(error):
    return "Sorry, we arent ready for that probe yet"

@route('/<dbcode>/<entity>')
def get_info(dbcode, entity):
    
    strfilename = dbcode + ".json"
    print strfilename

    with open(strfilename) as datafile:
        data = json.load(datafile)

    probes = data[entity.decode('utf-8')]['startdaydict'.decode('utf-8')].keys()

    possible_probes = []

    for each_probe in probes:
        first_day_string = str(data[entity.decode('utf-8')]['startdaydict'.decode('utf-8')][each_probe]) 
        last_day_string = str(data[entity.decode('utf-8')]['enddaydict'.decode('utf-8')][each_probe])   
        name = str(each_probe)
        human_readable = name + " is available from " + first_day_string + " to " + last_day_string 
        
        possible_probes.append(human_readable)

    return template('<p><b> check out this lovely list of probes:</b></p>{{lop}}', lop = possible_probes)

@route('/<dbcode>/<entity>/<probecode>')
def glitchify(dbcode, entity, probecode):
    strfilename = dbcode + ".json"

    try:
        u = yaml.load(open(dbcode+'.yml'),'r'))
    except Exception:
        return "Error, yaml file is not found"


run(host='localhost', port=8080, reloader = True)

# dir_map = {'HT004.yml': 'HT004', 'MS001.yml': 'MS001', 'MS005.yml':'MS005', 'MS043.yml':'MS043'}

# def break_url(url)
#     o = urlparse('http://andrewsforest.oregonstate.edu/lter/data/attributes.cfm?dbcode=MS043&entnum=17&topnav=8')
#     query = parse_qs(o.query)

#     my_attribute = query['dbcode'] + query['entnum']
#     my_db = query['dbcode']

#     return my_attribute, my_db


# def get_yamls():
#     ''' get yaml backbone file'''
#     if my_db == 'MS043':
#         u = yaml.load(open('MS043.yml','r'))
#     elif my_db == 'MS001':
#         u = yaml.load(open('MS001.yml','r'))
#     elif my_db == 'TW006':
#         u = yaml.load(open('TW006.yml','r'))
#     elif my_db == 'MS005'
#         u = yaml.load(open('TW006.yml','r'))
#     elif my_db == 'TV025'
#         u = yaml.load(open('TV025.yml','r'))
#     elif my_db == 'HT004'
#         u = yaml.load(open('HT004.yml','r'))
#     else:
#         pass

#     return u

