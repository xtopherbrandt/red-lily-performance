from flask import Flask, redirect, url_for, request, session, render_template
from stravalib import Client
from stravalib.exc import ActivityUploadFailed
import os, sys
from requests.exceptions import HTTPError
from datetime import datetime, date, timedelta
import dateutil.parser
import pickle
from units import *
import xml.etree.ElementTree as ET
import shutil
import json
from units import *
from units.predefined import define_units
import re
from FlaskWebProject1 import app
from azure.storage.table import TableService, Entity
from azure.common import AzureMissingResourceHttpError
from format_functions import *

app.secret_key = os.urandom(24)

#_redirect_url = 'http://localhost:5555/exchange'
_redirect_url = 'http://red-lily-performance.azurewebsites.net/exchange'
_client_id = 16323
_client_secret = 'acc979731b8be9933f46ab89f9d8094c705a3503'

_data_donor_program_active = True

# Connection to the Azure table storage
table_service = TableService( account_name='redlily', account_key='VoC60FOlqCpROjKuJ8icCiGEDuXcERTZdQaUeoBsS4eNDjSr0uL6/NtpZssis44Av39lNWyzNFVQ1cUO3h6MuA==')
_strava_partition_key = 'strava.com'

# Output formatting functions for Jinga
app.jinja_env.filters['datetime'] = format_datetime

@app.route('/')
@app.route('/home')
def home():
    # if have an active session, proceed to the races page
    if 'access_token' in session:
        return redirect(url_for('races'))
    else:
        # if they're previously authorized, their athlete id will be set in a cookie
        authenticated = request.cookies.get('authenticated')

        # if we found it, re-authorize
        if authenticated is not None:
            return redirect(url_for('authorize'))
        else:
            # This is a new recruit, so send them to the initial page
            return render_template('index.html',title='Red Lily', year=datetime.now().year, message='Fine Tune Performance')
    
@app.route('/contact')
def contact():
    """Renders the contact page."""
    return render_template(
        'contact.html',
        title='Contact Red Lily',
        year=datetime.now().year,
        message='Fine Tune Performance'
    )

@app.route('/about')
def about():
    """Renders the about page."""
    return render_template(
        'about.html',
        title='About Red Lily',
        year=datetime.now().year,
        message='Fine Tune Performance'
    )

@app.route('/races')
def races():
    
    if 'access_token' not in session:
        return redirect(url_for('home'))
    
    client = Client( session['access_token'] )

    if 'races' in session :
        races = session['races']
    else:
        races = retrieveRaces(session['athlete_id'])
        session['races'] = races
    
    """Renders the races page."""
    return render_template( 'races.html', title='My Races', year=datetime.now().year, message='Fine Tune Performance', race_list=races )

@app.route('/admin')
def admin():

    if 'access_token' not in session:
        return redirect(url_for('home'))
    
    client = Client( session['access_token'] )

    if client.get_athlete().email == 'xtopher.brandt@gmail.com':
        return render_template('commands.htm')
    else:
        return redirect(url_for('races'))

@app.route('/authorize')
def authorize():
        
    client = Client( )

    url = client.authorization_url(client_id=_client_id, redirect_uri=_redirect_url, scope='view_private')
    return redirect(url)

@app.route('/exchange', methods=['GET'])
def exchange():
            
    client = Client( )

    code = request.args.get('code')
    access_token = client.exchange_code_for_token(client_id=16323, client_secret=_client_secret, code=code)

    # Mark the session as logged in and store the access_token
    session['access_token'] = access_token

    strava_user = client.get_athlete( )
    client.access_token = access_token
    athlete_id = strava_user.id
    session['athlete_id'] = athlete_id

    # start the response so that we can set a cookie
    response = app.make_response( redirect(url_for('races')) )

    cookie_expiry = datetime.now() + timedelta(weeks = 52)
    
    response.set_cookie(key='authenticated', value='true', expires= cookie_expiry, httponly=True)

    # try to find the user in our database
    user = findUser( athlete_id )
    
    # if we didn't find them, add them
    if user == None :
        enrolment_date = datetime.utcnow()
        data_donor = _data_donor_program_active
        user = { 'PartitionKey': _strava_partition_key, 'RowKey': str(athlete_id), 'athlete_id':str(athlete_id), 'firstname':strava_user.firstname, 'lastname':strava_user.lastname, 'email':strava_user.email, 'access_token':access_token, 'enrolment_date':enrolment_date.isoformat(), 'data_donor':str(data_donor)}
        table_service.insert_entity( 'Users', user )
    elif 'access_token' not in user or user['access_token'] != access_token:
        # if we do have them, ensure we have their current access token
        user['access_token'] = access_token
        table_service.update_entity('Users', user )

    return response

@app.route('/buildRaceDataset', methods=['GET'])
def buildRaceDataset():
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        return redirect(url_for('home'))
    
    client = Client( session['access_token'] )

    athlete_id = session['athlete_id']

    race_list = []

    try:
        print 'Building dataset for {0}...'.format( athlete_id )
        activity_set = client.get_activities( )
        
        for activity in activity_set:
            if activity.workout_type == '1' :
                print 'Found Race : {0} - {1}'.format( activity.name, activity.start_date)
                race_list.append({"start_date":activity.start_date, "name":activity.name, "distance":float(activity.distance)})
                saveRace(athlete_id, activity)
        
        #session['race_list'] = race_list

        return redirect( url_for( 'races') )
    except Exception as e:
        print e
        # swallow errors
        return redirect( url_for( 'races') )

@app.route('/activities', methods=['GET'])
def activities():
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))

    
    client = Client( session['access_token'] )

    try:
        activity_set = client.get_activities( before='2014-12-01', after='2008-01-01' )
        activities = []
        for activity in activity_set:
            activities.append({'id':activity.id, 'external_id':activity.external_id, 'distance':activity.distance, 'name':activity.name, 'desc':activity.description})

        print activities[0]
        return render_template('show_activities.html', activites=activities)
    except HTTPError:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    except Exception as e:
        print e
        return str(e)

@app.route('/generateDataPoints', methods=['GET'])
def generateDataPoints():
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))

    
    client = Client( session['access_token'] )

    try:
        print 'Searching...'
        activity_set = client.get_activities( )
        
        for activity in activity_set:
            if activity.workout_type == '1' :
                print 'Found Race : {0} - {1}'.format( activity.name, activity.start_date)
                #saveDataPoint(activity)
        return "OK"
    except Exception as e:
        print e
        return str(e)

@app.route('/activities/stream', methods=['GET'])
def activitiesStream():
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))

    
    client = Client( session['access_token'] )

    try:
        stream = client.get_activity_streams(activity_id = 856784749, types=['time','distance','altitude', 'velocity_smooth','heartrate','cadence','grade_smooth'], resolution='high')
                
        return "OK"
    except HTTPError:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    except Exception as e:
        print e
        return str(e)


@app.route('/activities/files/batch')
def activitiesFilesBatch():
    '''
    Helper function to process a batch of local files

    DO NOT DEPLOY THIS METHOD TO PRODUCTION

    BIG SECURITY RISK
    '''
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    
    client = Client( session['access_token'] )

    # Create a mapping between the garmin ID and the strava ID for all activities
    # this was added to allow re-uploading of activity files
    activity_map = {}
    activity_set = client.get_activities( )
    for activity in activity_set:
        activity_map[activity.external_id] = activity.id

    # Get the file name from the query string
    file_directory = request.args.get('dir')
    completed_files = []

    files = os.listdir(file_directory)

    # For each file in the directory
    for gpx_file in files:
        # If this is a file
        if ( os.path.isfile( os.path.join(file_directory, gpx_file) ) ):
            
            # If this file has already been uploaded, delete it and re-upload
            if gpx_file in activity_map:
                print 'Deleting activity {0}:{1}'.format(gpx_file, activity_map[gpx_file] )
                client.delete_activity( activity_map[gpx_file] )

            try:
                # Process it
                time, name, desc, garmin_type, strava_type, external_id, data_type = processGpxFile(os.path.join(file_directory, gpx_file))
                
                # Move it to Done
                shutil.move(os.path.join(file_directory, gpx_file), os.path.join(file_directory, 'Done'))
            
                # Record it
                f = {'fileName':gpx_file, 'name': name, 'desc': desc, 'garmin_type': garmin_type, 'strava_type': strava_type, 'external_id': external_id }
                completed_files.append(f)
            except ActivityUploadFailed:
                # skip failures, these are usually duplicates
                 
                # Move it to Unprocessed
                shutil.move(os.path.join(file_directory, gpx_file), os.path.join(file_directory, 'Unprocessed'))
                pass
            except Exception as e:
                import traceback
                print 'Exception while processing file:{0} : {1}'.format(gpx_file, e)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print traceback.format_exception(exc_type, exc_value, exc_traceback )
                return str(e)

    return render_template('show_uploaded_files.html', files=completed_files)

@app.route('/activities/files/upload')
def activitiesFilesUpload():
    name = 'Not Found'
    files = []

    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    
    client = Client( session['access_token'] )

    try:
        # Get the file name from the query string
        gpx_file = request.args.get('file')

        # process and upload the file
        time, name, desc, garmin_type, strava_type, external_id, data_type = processGpxFile( gpx_file )
        f = {'fileName':gpx_file, 'name': name, 'desc': desc, 'garmin_type': garmin_type, 'strava_type': strava_type, 'external_id': external_id }
        files.append(f)
        return render_template('show_uploaded_files.html', files=files)
    except HTTPError:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    except Exception as e:
        print e
        return str(e)

@app.route('/activities/files/recreate')
def activitiesFilesRecreate():
    # if we don't have an access token, reauthorize first
    if 'access_token' not in session:
        print "Reauthorizing..."
        return redirect(url_for('authorize'))
    
    client = Client( session['access_token'] )

    # Create a mapping between the garmin ID and the strava ID for all activities
    # this was added to allow re-uploading of activity files
    # 
    # The external id in Strava is set to the file name if the activity was uploaded either manually or through the API
    #  it is set to garmin_push_{garmin_id} if the activity was pushed from garmin directly
    #  and for a short period of time between Nov and Dec 2015 it was set to the activity start time, 
    #   and to make things worse, the start time given to Strave by Garmin could differ by seconds from the start time provided in garmin downloads  :-(
    activity_map = {}
    activity_set = client.get_activities( )
    for activity in activity_set:
        if activity.external_id is not None:
            ids = re.compile('\d{5,}').findall(activity.external_id)
            if (len(ids) != 1):
                timestamp_id = activity.start_date.strftime("%Y-%m-%d %H:%M")
                activity_map[timestamp_id] = activity.id
            else:
                activity_map[ids[0]] = activity.id

    # Get the file name from the query string
    file_directory = request.args.get('dir')
    completed_files = []

    files = os.listdir(file_directory)

    completed_files =  []

    # For each file in the directory
    for json_file in files:
        
        # If this is a file
        if ( os.path.isfile( os.path.join(file_directory, json_file) ) ):
            
            # If this activity isn't in the map 
            # then try to create an activity from the json data
            garmin_id = re.compile('\d{4,}').findall(json_file)[0]

            if garmin_id not in activity_map :
                
                # Open the file and load the data
                with open( os.path.join(file_directory, json_file), 'r') as activity_file:
                    activity = json.load( activity_file )

                print activity['activitySummary']['BeginTimestamp']['value']
                print dateutil.parser.parse(activity['activitySummary']['BeginTimestamp']['value']).strftime("%Y-%m-%d %H:%M")

                # double check that an activity with this start date isn't in the activity map
                if dateutil.parser.parse(activity['activitySummary']['BeginTimestamp']['value']).strftime("%Y-%m-%d %H:%M") in activity_map :
                    print 'Activity already exists with timestamp as external ID'
                                     
                    # Move it to Unprocessed
                    shutil.move(os.path.join(file_directory, json_file), os.path.join(file_directory, 'Unprocessed'))
                else:

                    print 'Creating activity for {0}'.format( json_file )

                    define_units()

                    name=activity['activityName']
                    activity_type = garminTypeToStravaType(activity['activityType']['key'])
                    start_date_local = datetime.strptime( activity['activitySummary']['BeginTimestamp']['display'], "%a, %b %d, %Y %I:%M %p" )
                    
                    if 'SumDuration' in activity['activitySummary']:
                        elapsed_time = int(round(float(activity['activitySummary']['SumDuration']['value']),0))
                    else:
                        elapsed_time = 0

                    description = activity['activityDescription'] + 'From Garmin: {0}'.format(garmin_id)
                    
                    if 'SumDistance' in activity['activitySummary']:
                        distance = unit(activity['activitySummary']['SumDistance']['unitAbbr'])(float(activity['activitySummary']['SumDistance']['value']))
                    else:
                        distance = 0

                    print name, activity_type, start_date_local, elapsed_time, description, distance

                    try:
                        client.create_activity(name=name, activity_type=activity_type, start_date_local=start_date_local, elapsed_time=elapsed_time, description=description, distance=distance)
                                        
                        # Move it to Done
                        shutil.move(os.path.join(file_directory, json_file), os.path.join(file_directory, 'Done'))

                        # Record it
                        f = {'fileName':json_file, 'name': name, 'desc': description, 'garmin_type': activity['activityType']['key'], 'strava_type': activity_type, 'external_id': '' }
                        completed_files.append(f)
                                
                    except Exception as e:
                        import traceback
                        print 'Exception while processing file:{0} : {1}'.format(gpx_file, e)
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        print traceback.format_exception(exc_type, exc_value, exc_traceback )
                        return str(e)
            else:
                                    
                    # Move it to Unprocessed
                    shutil.move(os.path.join(file_directory, json_file), os.path.join(file_directory, 'Unprocessed'))


    return render_template('show_uploaded_files.html', files=completed_files)

def findUser(athlete_id):
    user = None
    try:
        user = table_service.get_entity(table_name='Users', partition_key=_strava_partition_key, row_key=athlete_id )
        #userList = table_service.query_entities(table_name='Users', filter="PartitionKey eq '{0}' and RowKey eq '{1}'".format(_strava_partition_key, athlete_id) )
 
    except AzureMissingResourceHttpError:
        print 'User not found'
        
    return user

def retrieveRaces(athlete_id):
    #races = table_service.query_entities(table_name='Races', filter="PartitionKey eq '{0}'".format(athlete_id))
    client = Client( session['access_token'] )

    athlete_id = session['athlete_id']

    races = []

    try:
        activity_set = client.get_activities( )
        
        for activity in activity_set:
            if activity.workout_type == '1' :
                races.append({"start_date":activity.start_date, "name":activity.name, "distance":float(activity.distance)})
        
    except Exception as e:
        print e
        pass

    return races

def saveRace(athlete_id, raceActivity):

    race = {'PartitionKey':str(athlete_id), 'RowKey':str(raceActivity.id), 'Race_Date':raceActivity.start_date, 'Race_Name':raceActivity.name, 'Race_Distance':float(raceActivity.distance)}
    table_service.insert_entity('Races', race )



def processGpxFile(gpx_file_name):
    # Open the file for read processing
    with open( gpx_file_name, 'r') as activity_file:
        # Use XPath to find some of the details like name, description and type
        ET.register_namespace('', "http://www.topografix.com/GPX/1/1")
        root = ET.parse(activity_file).getroot()
        ns = {'gpxns':'http://www.topografix.com/GPX/1/1'}
        time_elem = root.find('gpxns:metadata/gpxns:time', ns)
        if time_elem is not None :
            time = time_elem.text
        else :
            time = ''
        name_elem = root.find('gpxns:trk/gpxns:name', ns)
        if name_elem is not None :
            name = name_elem.text
        else:
            name = ''
        desc_elem = root.find('gpxns:trk/gpxns:desc', ns)
        if desc_elem is not None :
            desc = desc_elem.text
        else:
            desc = ''
        garmin_type_elem = root.find('gpxns:trk/gpxns:type', ns)
        if garmin_type_elem is not None :
            garmin_type = garmin_type_elem.text
        else :
            garmin_type = ''
        external_id = gpx_file_name.split('_')[1].split('.')[0]
        data_type = gpx_file_name.split('_')[1].split('.')[1]

        # Convert the garmin type to a strava type
        strava_type = garminTypeToStravaType( garmin_type )

        # Replace all of the <ns3:TrackPointExtension> elements with <heartrate> element
        ns3 = {'extns':'http://www.garmin.com/xmlschemas/TrackPointExtension/v1'}
        extenstions_elem = root.findall('.//gpxns:trkpt/gpxns:extensions', ns)
        for elem in extenstions_elem:
            trackpointExts_elem = elem.find('./extns:TrackPointExtension', ns3)
            if trackpointExts_elem is not None:
                hr_elem = trackpointExts_elem.find('./extns:hr', ns3)
                if hr_elem is not None:
                    hr = hr_elem.text
                    ET.SubElement(elem, 'heartrate').text = hr
                elem.remove(trackpointExts_elem)

        # Log what we know
        print '{0}: {1} {2} {3} {4}-->{5} {6} {7}'.format(gpx_file_name, time, name, desc, garmin_type, strava_type, external_id, data_type)

    # Now re-open it to save the modified GPX tree
    with open( gpx_file_name, 'w+') as activity_file:

        # Save the tree to the file
        tree = ET.ElementTree(root)
        tree.write(activity_file, xml_declaration=True, encoding='UTF-8')

        # Move the file pointer back to the start
        activity_file.seek(0)
        
        # Upload the file and wait for up to 1 min for the response to come back
        client.upload_activity( activity_file=activity_file, data_type=data_type, name=name, description=desc, activity_type=strava_type ).wait(timeout=60)

    return time, name, desc, garmin_type, strava_type, external_id, data_type


def garminTypeToStravaType( garminType ):
    if type(garminType) != str and type(garminType) != unicode:
        app.logger.debug("invalid type {0}".format( type(garminType)))
        return None
    elif garminType.find('running') != -1:
        return 'run'
    elif garminType.find('cycling') != -1:
        return 'ride'
    elif garminType.find('biking') != -1:
        return 'ride'
    elif garminType == 'BMX':
        return 'ride'
    elif garminType.find('swimming') != -1:
        return 'swim'
    elif garminType == 'hiking':
        return 'hike'
    elif garminType.find('walking') != -1:
        return 'walk'
    elif garminType == 'cross_country_skiing':
        return 'nordicski'
    elif garminType == 'skate_skiing':
        return 'nordicski'
    elif garminType == 'resort_skiing_snowboarding':
        return 'alpineski'
    elif garminType == 'skating':
        return 'iceskate'
    elif garminType == 'inline_skate':
        return 'inlineskate'
    elif garminType == 'backcountry_skiing_snowboarding':
        return 'backcountryski'
    else:
        return None
