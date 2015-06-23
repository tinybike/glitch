import requests
import json
import datetime
import csv
from bottle import route, run, request, template, error, static_file, get, post
from logic_glitch import SmartGlitcher, Glitcher, drange, connect

@route('/')
def index():
    return('<h1> welcome to glitch, please choose you a database </h1>')

@error(404)
def error404(error):
    return "Sorry, we arent ready for that probe yet"

@error(403)
def error403(error):
    return "Sorry, you cant access that resource"

@route('/<filename:re:.*\.js>')
def javascripts(filename):
    return static_file(filename, root='js/')

@route('/<filename:re:.*\.css>')
def stylesheets(filename):
    return static_file(filename, root='css/')

@get('/<filename:re:.*\.(jpg|png|gif|ico)>')
def images(filename):
    return static_file(filename, root='img/')

@get('/<filename:re:.*\.(eot|ttf|woff|svg)>')
def fonts(filename):
    return static_file(filename, root='static/fonts')

@get('/<dbcode>')
def show_json(dbcode):
    with open(dbcode +'.json','r') as datafile:
        data = json.load(datafile)

        # put entities (i.e. MS04311) in alphabet order
        study_codes = sorted(data.keys())
        print study_codes
        # generate nested menus string for entities (MS043 > MS04311 > AIRCEN01)
        outer_list = []

        # add a tag to start the menu
        outer_list.append("<ul id=\"menu\">")
        
        # for each entity...
        for each_code in study_codes:

            inner_list = []

            if len(data[each_code]['startdaydict'].keys()) != []:

                # i.e. <li> MS04310 
                formatted_study_code = "<li><a href=\"#\" id=\"main-list\">" + each_code + "<ul>"

                # identify the probes
                for each_probe in sorted(data[each_code]['startdaydict'].keys()):

                    # try to find an ending day in the other lookup
                    try:
                        final_found = data[each_code]['enddaydict'].values()[0]
                    
                    except Exception:
                        final_found = "None"

                    # combine the two days
                    formatted_probe_name = "<li a href=\"#\" id=" + each_probe + ">" + each_probe + " (starts: " + data[each_code]['startdaydict'].values()[0] + ", ends: " + final_found + ")</a></li>"
            
                    inner_list.append(formatted_probe_name)
                
                inner_list.append("</ul>")

                # join the innner list to a string
                inner_entities = "".join(inner_list)

                # join the formatted study code to the inner entities it belongs with
                formatted_nests = formatted_study_code + " " + inner_entities
            
            else:
                formatted_study_code = "<li class=\"ui-state-disabled\">" + each_code + "</li>"
                outer_list.append("li> None available </li></ul>")

            outer_list.append(formatted_nests)
        outer_list.append("</ul>")
        drop_down_entities = "".join(outer_list)

        myprint = """
                    <!doctype html>
                    <html lang="en">
                    <head>
                    <meta charset="utf-8">
                    <meta http-equiv="X-UA-Compatible" content="IE=edge">
                    <meta name="viewport" content="width=device-width, initial-scale=1">
                    <meta name="description" content="A simple, client centric interface for newGlitch">
                    <meta name="author" content="The Saviors of SQL, Fox, Don, and Hans">
                    <title>newGlitch | by Fox, Don, and Hans </title>
                    <link type="text/css" href="bootstrap.min.css" rel="stylesheet">
                    <link type="text/css" href="bootstrap-theme.min.css" rel="stylesheet">
                    <link rel="stylesheet" href="//code.jquery.com/ui/1.11.4/themes/smoothness/jquery-ui.css">
                    <script src="//code.jquery.com/jquery-1.10.2.js"></script>
                    <script src="//code.jquery.com/ui/1.11.4/jquery-ui.js"></script>

                    <script>
                    $(function() {
                        $( "#menu" ).menu();
                    });
                    </script>

                    <script>
                    $(function() {
                        $( "#start_datepicker" ).datepicker();
                    });
                    </script>

                    <script>
                    $(function() {
                        $( "#end_datepicker" ).datepicker();
                    });
                    </script>


                    <style>
                    .ui-menu { 
                        width: 200px; 
                    }
                    .ui-menu ui-widget ui-widget-content ui-front { 
                        width: 400px; 
                    }
                    </style>

                    </head>
                    <body role="document">

                        <nav class="navbar navbar-inverse navbar-fixed-top">
                        <div class="container">
                        <div class="navbar-header">
                            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                                <span class="sr-only">Toggle navigation</span>
                                <span class="icon-bar"></span>
                                <span class="icon-bar"></span>
                                <span class="icon-bar"></span>
                            </button>
                            <a class="navbar-brand" href="#">newGlitch</a>
                        </div>
                        </div>
                        </nav>

                    <div class="col-xs-12" style="height:100px;"></div>
                    <div class = "container">
                        <div class = "row">
                        <div class = "col-md-7">
                        
                            """ + drop_down_entities + """ 
                        </div>
                    
                        <div class = "col-md-5">
                        <form action = "/"""+dbcode+"""", method="post">
                        <h3> PROBE: <input name = "menu-selection" type="text" id = "menu-selection" readonly></h3>
                        <label> Start Date </label>
                        <p>Date: <input name = "startdate" type="text" id="start_datepicker">
                        <label> Start Time </label>
                        <p>Time: <input name = "starttime" type="text" id="start_time"></p>
                        <label> End Date </label>
                        <p>Date: <input name ="enddate" type="text" id="end_datepicker">
                        <label> End Time </label>
                        <p>Time: <input name ="endtime" type="text" id="end_time"></p>
                        <label> Interval </label>
                        <p>Any number of minutes: <input name ="interval" type="text" id="interval"></p>
                        <input name ="entity" type="hidden" id="entity">
                        <button type="submit" class="btn btn-lg btn-primary" id="submit">Submit</button>
                        </form>
                        </div><!-- /date section -->

                    </div><!-- /row -->

                    <footer>
                        <p>&copy; "The Fault is in the FoxPro"</p>
                    </footer>
                    </div> <!-- /container -->
                    <script>
                    $(document).ready(function() {
                      $( "li.ui-menu-item" ).click(function() {
                        var myName = $('li.ui-menu-item.ui-state-focus').get(0).id;                
                        console.log(myName);
                        var myDad = $('li.ui-menu-item.ui-state-focus').parent().parent().get(0).innerText;
                        var myDadName = myDad.substr(0,7);
                        console.log(myDad.substr(0,7));
                        
                        // set the menu selection to my name
                        $("#menu-selection").text(myName);
                        $("#menu-selection").val(myName);
                        $("#entity").val(myDadName);
                        });
                    });
                    </script>
                    </body>
                    </html>"""

    return myprint

def parse_time(some_time):
    list_time = some_time.split(':')
    hours = int(list_time[0])
    minutes = int(list_time[1])
    return hours, minutes


@route('/<dbcode>', method='POST')
def do_glitch(dbcode):
    startdate = request.forms.get('startdate')
    enddate = request.forms.get('enddate')
    probe = request.forms.get('menu-selection')
    starttime = request.forms.get('starttime')
    endtime = request.forms.get('endtime')
    interval = request.forms.get('interval')
    entity = request.forms.get('entity')

    print entity

    hstart, mstart = parse_time(starttime)
    dstart = datetime.datetime.strptime(startdate,'%m/%d/%Y')
    clean_start = datetime.datetime(dstart.year, dstart.month, dstart.day, hstart, mstart, 0)

    hend, mend = parse_time(endtime)
    dend = datetime.datetime.strptime(enddate,'%m/%d/%Y')
    clean_end = datetime.datetime(dend.year, dend.month, dend.day, hend, mend, 0)

    rep_start = datetime.datetime.strftime(clean_start,'%Y-%m-%d %H:%M:%S')
    rep_end = datetime.datetime.strftime(clean_end, '%Y-%m-%d %H:%M:%S')


    TestG = SmartGlitcher(entity, probe, rep_start, rep_end, int(interval))
    output_csv = TestG.tocsv('my_test_glitch_from_app.csv')
    
    web_csv_write = "".join(output_csv)
    
    try:
        return "<h3> newGlitch assembled values from " + rep_start + " to " + rep_end + " on " + probe + " glitched on an interval of " + interval + "minutes! </h3>" + web_csv_write
    
    except Exception:
        # for debugging-- print to screen
        print rep_start, rep_end, probe, interval
        return  "<h3> newGlitch assembled values from " + rep_start + " to " + rep_end + " on " + probe + " glitched on an interval of " + interval + "minutes! </h3>" + web_csv_write

run(host='localhost', port=8080, reloader = True, debug=True)
