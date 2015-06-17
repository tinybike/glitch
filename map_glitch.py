#!usr/bin/python

import pymssql
import time
import datetime
import itertools
import sys
import yaml
import json

# class SchemaInfoBuild(object):

#   def __init__(self):
#     """ Class for building up the background information that only needs to be run once or twice. Ultimately move all the functions below into this."""
#     _, self.cursor = connect()
#       self.possible = ['MS043', 'MV001', 'TW006', 'MS001', 'MS005', 'TV025', 'HT004']
#       self.od = all_build_dict()
 
#   def all_build_dict(self):  
#     """ Possible schema which have a column containing 'probe'. Add to this list to make a more robust search space"""

#     for each_schema in self.possible:
#       if each_schema not in od:
#           od[each_schema] = get_entity_id(each_schema)
#       else:
#         pass
#     return od


def connect():
    ''' connect to the fsdbdata database'''

    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor

def get_entity_id(cursor, databaseid):
  ''' from entity table, get the entity id number, description, entity name, beginning and end date, etc. based on the table name we are familiar with, such as MS043 '''

  dbid_dict = {}

  define_query = "select entity_id, entity_file_name, entity_number, entity_title, beginning_date, ending_date from ltermeta.dbo.entity where entity_file_name like \'" + databaseid + "%\'"

  cursor.execute(define_query)

  for row in cursor:

    if str(row[1]).rstrip() not in dbid_dict:
      
      dbid_dict[str(row[1]).rstrip()] = {'entity_id':str(row[0]).rstrip(), 'entity_number': str(row[2]).rstrip(), 'entity_title': str(row[3]).rstrip(), 'beginning_date': str(row[4]).rstrip, 'ending_date': str(row[5]).rstrip()}
    else:
      pass

  return dbid_dict

def all_build_dict(cursor):  
    """ Possible schema which have a column containing 'probe'. Add to this list to make a more robust search space"""
    
    possible = ['MS043', 'MV001', 'TW006', 'MS001', 'MS005', 'TV025', 'HT004']

    od = {}

    for each_schema in possible:
      if each_schema not in od:
          od[each_schema] = get_entity_id(cursor, each_schema)
      else:
        pass
    return od


def all_get_references(cursor, od):
    """ getting all the possible probe, mean, and method names """

    # for example, each_schema is like 'MS043' etc.
    for each_schema in od.keys():

        schema_d = {}
        
        # for example, ['MS04311', 'MS04312', etc] are in 'MS043'
        attr_list = od[each_schema].keys()

        for my_attribute in attr_list:
            # for example, 1731 may be MS04311 entity
            my_entity_id = str(od[each_schema][my_attribute]['entity_id'])
            
            # the list of attributes
            loa = get_attribute_id_for_probe(cursor, my_entity_id)
            
            # the list of names containing attributes we desire (PROBE, MEAN, etc.)
            try:
                lon = get_attribute_names_for_probe_and_site(cursor, loa)
            except Exception:
                print "there is no metadata for table " + my_attribute
                continue

            # skip those which do not have "DATE_TIME"
            print "checking for the existance of hr data and mean data"
            

            if "DATE_TIME" not in lon and "DATETIME" not in lon:
                continue

            elif "DATE_TIME" in lon or "DATETIME" in lon:
                # what is the date word
                date_word = [x for x in lon if 'DATE' in x][0]
                
                # which is probe/probe_type, mean/meanflag
                probe_word = [x for x in lon if 'PROBE' in x][0]
                mean_words = [x for x in lon if 'MEAN' in x]
                method_words =[x for x in lon if 'METHOD' in x]
                
                if len(mean_words) >= 1:
                   
                    print "yay, we have mean words"
                    print mean_words
                    print method_words

                    # this only needs to be processed one time, so let's get it right
                    startdaydict, enddaydict = epic_query(cursor, my_attribute, probe_word, date_word)
                
                if my_attribute not in schema_d:
                    schema_d[my_attribute] ={'startdaydict': startdaydict, 'enddaydict': enddaydict, 'probe_word': probe_word, 'mean_words': mean_words, 'method_words': method_words, 'date_word': date_word}
                elif my_attribute in schema_d:
                    print "my attribute should not already be here?"
                    import pdb; pdb.set_trace()

                else:
                    continue  
            else:
                print "It is only daily for the " + my_attribute

        write_schema_to_yaml(each_schema, schema_d)
        write_schema_to_json(each_schema, schema_d)

def get_attribute_id_for_probe(cursor, my_entity_id):
    """ get the probe codes from attribute"""

    define_query = "select attribute_id from ltermeta.dbo.entity_attribute where entity_id = \'" + my_entity_id +  "\'"

    cursor.execute(define_query)

    list_attributes = []

    for row in cursor:
        try:
            list_attributes.append(str(row[0]))
        except Exception:
            import pdb; pdb.set_trace()
    return tuple(list_attributes)


def epic_query(cursor, my_attribute, probe_word, date_word):
    """ generates distinct start and ending dates for each probe on each attribute if it is hr probe"""
    starts_by_probe = {}
    ends_by_probe = {}

    query = "with summary as (select p." + probe_word + ", p." + date_word + ", row_number() over(partition by p." + probe_word + " order by p." + date_word + " asc) as rk from fsdbdata.dbo." + my_attribute + " p) select s.* from summary s where s.rk = 1"
    cursor.execute(query)
    for row in cursor:
        if str(row[0]) not in starts_by_probe:
            starts_by_probe[str(row[0])] = str(row[1])
        elif str(row[0]) in starts_by_probe:
            print "error, probe already listed"

    query = "with summary as (select p." + probe_word + ", p." + date_word + ", row_number() over(partition by p." + probe_word + " order by p." + date_word + " desc) as rk from fsdbdata.dbo." + my_attribute + " p) select s.* from summary s where s.rk = 1"
    cursor.execute(query)
    for row in cursor:
        if str(row[0]) not in ends_by_probe:
            ends_by_probe[str(row[0])] = str(row[1])
        elif str(row[0]) in ends_by_probe:
            print "error, probe already listed"

    return starts_by_probe, ends_by_probe

def write_schema_to_yaml(this_schema, schema_dict):

    with open(this_schema + '.yml', 'w') as outfile:
        outfile.write( yaml.dump(schema_dict, default_flow_style=True) )

    print "I have dumped the output to a yaml"

def write_schema_to_json(this_schema, schema_dict):

    with open(this_schema + '.json', 'w') as outfile:
        json.dump(schema_dict, outfile)

def process_epic_to_html(startdaydict, enddaydict):
    pass

# def query_fsdb_for_data(my_attribute, probe_word, mean_words):

#     query = "select " + probe_word + ", ".join(mean_words) + " from fsdbdata.dbo." + my_attribute + " order by date_time >= \'" + startdate + "\' and  date_time < \'" + enddate + "\' and "+ probetype + " like \'" + args[0] + "\'"

# def get_attribute_id_for_probe(cursor, my_attribute, dbid_dict):
#     """ get the probe codes from attribute"""

#     # filler
#     # my_attribute = 'MS04311'
#     my_entity_id = str(dbid_dict[my_attribute]['entity_id'])

#     define_query = "select attribute_id from ltermeta.dbo.entity_attribute where entity_id = \'" + my_entity_id +  "\'"

#     cursor.execute(define_query)

#     list_attributes = []

#     for row in cursor:
#         try:
#             list_attributes.append(int(row[0]))
#         except Exception:
#             import pdb; pdb.set_trace()
#     return tuple(list_attributes)

def get_attribute_names_for_probe_and_site(cursor, list_attributes):
    """ get the probe code name and mean things name from attribute table"""

    define_query = "select attribute_name from ltermeta.dbo.attribute where attribute_id in " + str(list_attributes)

    cursor.execute(define_query)

    attributes_we_want_to_glitch = []

    for index, row in enumerate(cursor):

        if "PROBE" in str(row[0]).rstrip():
            #attributes_we_want_to_glitch.append(int(list_attributes[index]))
            attributes_we_want_to_glitch.append(str(row[0]).rstrip())
            print "associating %s with %s" %(str(row[0]).rstrip(),list_attributes[index])

        if "METHOD" in str(row[0]).rstrip():
            #attributes_we_want_to_glitch.append(int(list_attributes[index]))
            attributes_we_want_to_glitch.append(str(row[0]).rstrip())
            print "associating %s with %s" %(str(row[0]).rstrip(),list_attributes[index])

        if "MEAN" in str(row[0]).rstrip():
            #attributes_we_want_to_glitch.append(int(list_attributes[index]))
            attributes_we_want_to_glitch.append(str(row[0]).rstrip())
            print "associating %s with %s" %(str(row[0]).rstrip(),list_attributes[index])

        if "DATE" in str(row[0]).rstrip():
            attributes_we_want_to_glitch.append(str(row[0]).rstrip())
            print "associating %s with %s" %(str(row[0]).rstrip(),list_attributes[index])

    return tuple(attributes_we_want_to_glitch)


def list_from_sublists(list_of_sublists):
  """ generates a lists from a bunch of sublists"""
  return list(itertools.chain.from_iterable(list_of_sublists))

def windy_attributes(name):
    """ test if any attribute contains wind data"""

    if 'WSPD' in name or 'WDIR' in name or 'WMAG' in name or 'WVc' in name or 'WIND' in name:
        return True

# def get_specific_probe_data(attribute_ids_for_glitching):

#     enumerated_domain_ids_for_glitching = []
#     define_query = "select enum_domain_id from ltermeta.dbo.attribute_enum_domain where attribute_id in " + attribute_ids_for_glitching

#     cursor.execute(define_query)

#     for row in cursor:
#         enumerated_domain_ids_for_glitching.append(int(row[0]))

#     return tuple(enumerated_domain_ids_for_glitching)

def big_master_list(enumerated_domain_ids_for_glitching):
    pass