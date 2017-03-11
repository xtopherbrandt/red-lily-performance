#!/usr/bin/python

import pickle
import sys, os
from units import * 
from datetime import datetime, date, timedelta
import time
import numpy as np
from stravalib import Client
from azure.storage.table import TableService, Entity
from azure.common import AzureMissingResourceHttpError
from FlaskWebProject1.views import _client_id, table_service, _strava_partition_key
from requests.exceptions import HTTPError

class DataProcessor:

    _strava_client = Client(rate_limit_requests=True)
    _velocity_bin_edges = None
    _cadence_bin_edges = None

    def __init__( self ):
        
        velocity_bins_Min = 1.0
        velocity_bins_Max = 10.0
        velocity_bins_Increment = 0.1

        # create an array of bin edges
        self._velocity_bin_edges = np.linspace( velocity_bins_Min, velocity_bins_Max, ((velocity_bins_Max - velocity_bins_Min)/velocity_bins_Increment) + 1 )

        cadence_bins_Min = 50
        cadence_bins_Max = 300
        cadence_bins_Increment = 2

        # create an array of bin edges
        self._cadence_bin_edges = np.linspace( cadence_bins_Min, cadence_bins_Max, ((cadence_bins_Max - cadence_bins_Min)/cadence_bins_Increment) + 1 )


    def Process(self, athlete_id = None):
        '''
        Processes the race and training data for each user. Normalizes the data and creates data points in our table.
        '''

        print 'Processing user data into normalized data points'
        users = table_service.query_entities(table_name='Users', filter=None, select='athlete_id, access_token, firstname, lastname')
        
        # For each user in the system
        for user in users:
            if athlete_id is None or ( athlete_id is not None and user['athlete_id'] == str(athlete_id) ) :
                print '    processing: {0} {1} ({2})'.format( user['firstname'], user['lastname'], user['athlete_id'] )
                
                self.ProcessUserData( user )

                    
        print 'Processing complete'
        print '================='


    @staticmethod
    def medianBinFunction (point0, point1):
        """
        Simple function to find the bin and duration given two consecutive points
        :param point0: a tuple (measuredValue, time)
        :param point1: a tuple (measuredValue, time)
        :return: binValue, duration
        """
        if point0 is None or point1 is None:
            return None, None

        binValue = ( point1[0] - point0[0] ) / 2 + point0[0]
        duration = point1[1] - point0[1]
        return binValue, duration

    @staticmethod
    def binMeasurements( bin_dict, measurementStream, timeStream, binPrefix, bin_edges ):
        """
        Iterates a stream of measurements, adding the time spent at each measured
        value to a bin for the measured value. Bins are added as items to the bin_dict
        with the name as binPrefix_measuredValue ie, velocity_3.5

        :param bin_dict: the dictionary to add the bins to. This is the data point.
        
        :param measurementStream: an array of measurement values. Must have the same length as timeStream.abs
        
        :param timeStream: an array of time values.
        
        :param binPrefix: the name of the bin class

        :param bin_edges: the array of bin edge values coming from a function like Numpy.linspace()
        """
        point0 = None
        point1 = None

        # Do a simple check to see if the bins have been set up yet
        testBinName = '{0}_{1}_{2}'.format(binPrefix, str(bin_edges[1]).replace('.','p') , str(bin_edges[2]).replace('.','p'))
        
        # if this bin is not in the dictionary then assume that it hasn't been set up yet and add all
        if testBinName not in bin_dict:
            #  decimal points are replaced with 'p' here because Azure tables don't like decimals in the property names
            for binIndex in range(bin_edges.size - 1):
                binName = '{0}_{1}_{2}'.format(binPrefix, str(bin_edges[binIndex]).replace('.','p') , str(bin_edges[binIndex+1]).replace('.','p'))
                bin_dict[binName] = 0

        # Go through the stream and get the bin value and duration for each increment
        for point in zip( measurementStream, timeStream ):
            point0 = point1
            point1 = point
            measurementValue, duration = DataProcessor.medianBinFunction( point0, point1 )

            # If we got a valid result, add it to the appropriate bin in the dictionary
            if measurementValue is not None:
                try:
                    binIndex = np.digitize( measurementValue, bin_edges )
                    if binIndex > 0 and binIndex < bin_edges.size :
                        binName = '{0}_{1}_{2}'.format(binPrefix, str(bin_edges[binIndex-1]).replace('.','p'), str(bin_edges[binIndex]).replace('.','p'))
                        bin_dict[binName] += duration
                        #print measurementValue, duration, binIndex, binName
                except IndexError:
                    print "Value {0} falls outside of bins for {1}".format(measurementValue, binPrefix)


    def ProcessUserData(self, user ):
        # Change the strava client access token
        self._strava_client.access_token = user['access_token']
        
        complete = False

        while not complete:
            try:
                activity_set = self._strava_client.get_activities( )
                athlete = self._strava_client.get_athlete()
                
                for activity in activity_set:
                    if activity.workout_type == '1' :
                        if not self._DataPointExists(athlete.id, activity.id) and not self._DataPointExcluded(athlete.id, activity.id):
                            print '      Processing Race : {0} - {1} ({2})'.format( activity.name, activity.start_date, activity.id )
                            self.ProcessDataPoint( athlete, activity)
                        else:
                            print '      Skipping Race : {0} - {1} ({2})'.format( activity.name, activity.start_date, activity.id )
                
                complete = True

            except HTTPError as e:

                if e.message.find('Rate Limit Exceeded') != -1:
                    print '******** Rate Limit Hit. Pausing, then restarting user processing'
                    time.sleep(60)
                else:
                    print '******** HTTPError: ', e

            except Exception as e:
                print '******** Exception while processing user data for {0}\n{1}'.format(user['athlete_id'], e.message )

    def ProcessDataPoint(self, athlete, raceActivity):

        # set the race date to the race start time at midnight UTC (to avoid funnies with start time of the race)
        raceDate = raceActivity.start_date.replace(hour=0,minute=0,second=0, microsecond=0)
        # set the last date of the data point to be 2 days prior to the race to avoid picking up warm-up rums
        lastDate = raceDate - timedelta(days = 2)
        # set the first date of the data point to 3 weeks prior
        firstDate = raceDate - timedelta(weeks=3)

        print "        Processing data point from: {0} to {1}".format(firstDate, lastDate)
        
        # get all of the activities within the period we've defined
        dataPointActivities = self._strava_client.get_activities( after=firstDate, before=lastDate )

        # summary data point information goes into datapointFeatures
        dataPointFeatures = { 'PartitionKey':athlete.id, 'RowKey':raceActivity.id }
        # velocity bins are in velocityFeatures
        velocityFeatures = {'PartitionKey':athlete.id, 'RowKey':raceActivity.id}
        # cadence bins are in cadenceFeatures
        cadenceFeatures = {'PartitionKey':athlete.id, 'RowKey':raceActivity.id}

        dataPointFeatures['athlete_id'] = athlete.id
        dataPointFeatures['gender'] = athlete.sex
        dataPointFeatures['race_date'] = raceActivity.start_date.isoformat()
        dataPointFeatures['race_distance'] = float( raceActivity.distance )
        dataPointFeatures['race_speed'] = float( raceActivity.average_speed )
        dataPointFeatures['race_speed_bin_index'] = int( np.digitize( float( raceActivity.average_speed ), self._velocity_bin_edges ) )
        dataPointFeatures['race_name'] = raceActivity.name
        dataPointFeatures['race_description'] = raceActivity.description
        dataPointFeatures['workout_count'] = 0
        dataPointFeatures['workout_distance'] = 0.0
        dataPointFeatures['workout_duration'] = 0
        dataPointFeatures['excluded'] = False

        # for each workout in the data dictionary
        #   add time spent to each measurement bin           
        workout_names={}

        for activity in dataPointActivities:
            if activity.type == 'Run':
                if not activity.flagged:

                    # some athletes seem to create multiple activities for a single workout
                    # need to filter these so that our workout count is only incremented once for above
                    # set of activities that appear to be the same
                    # so, we create a list of workout names for each date
                    if activity.start_date.date().isoformat() not in workout_names:
                        workout_names[activity.start_date.date().isoformat()] = []
                    
                    # if the current name of the current activity is NOT in the list for the activity's date
                    if activity.name not in workout_names[activity.start_date.date().isoformat()]:
                        # increment the workout count and add it to the list
                        dataPointFeatures['workout_count'] += 1
                        workout_names[activity.start_date.date().isoformat()].append(activity.name)

                    # save the activity to the audit data
                    auditDataPointActivity = { 'PartitionKey': raceActivity.id, 'RowKey': activity.id }                
                    auditDataPointActivity['workout_date'] = activity.start_date
                    auditDataPointActivity['workout_distance'] = float( activity.distance )
                    auditDataPointActivity['workout_duration'] = activity.elapsed_time.total_seconds()
                    auditDataPointActivity['workout_name'] = activity.name
                    auditDataPointActivity['workout_description'] = activity.description

                    table_service.insert_or_replace_entity( 'AuditDataPointActivities', auditDataPointActivity )

                    # add this workout's data to the datapoint
                    
                    dataPointFeatures['workout_distance'] += float( activity.distance )
                    dataPointFeatures['workout_duration'] += activity.elapsed_time.total_seconds()

                    # Try to get data streams for this workout
                    # Keep in mind that table entities can have a maximum of 252 properties
                    try:
                        workout_streams = self._strava_client.get_activity_streams(activity_id = activity.id, types=['time','distance','altitude', 'velocity_smooth','heartrate','cadence','grade_smooth'], resolution='high')

                        if 'velocity_smooth' in workout_streams:
                            DataProcessor.binMeasurements(velocityFeatures, workout_streams['velocity_smooth'].data, workout_streams['time'].data, 'velocity', bin_edges=self._velocity_bin_edges )
                        if 'cadence' in workout_streams:
                            DataProcessor.binMeasurements(cadenceFeatures, workout_streams['cadence'].data, workout_streams['time'].data, 'cadence', bin_edges=self._cadence_bin_edges )

                    except HTTPError as e:

                        # if we get a 429 throw it up to the next level to handle
                        if e.message.find('Rate Limit Exceeded') != -1:
                            raise HTTPError( e.message ) 
                        
                        # if we get a 404 ignore it and move on, it just means there were no streams
                        if e.message.find('404') != -1:
                            pass
                        
                        print '********  Error while trying to get streams for activity {0}\n{1}'.format(activity.id, e)
                    except Exception as e:
                        print '********  Error while trying to get streams for activity {0}\n{1}'.format(activity.id, e)
                else:
                    print '          FLAGGED activity :', activity.id

        print '        {0} workouts in this data point'.format(dataPointFeatures['workout_count'])

        
        try:
            table_service.insert_or_replace_entity( 'DataPoints', dataPointFeatures )
            table_service.insert_or_replace_entity( 'VelocityBins', velocityFeatures)
            table_service.insert_or_replace_entity( 'CadenceBins', cadenceFeatures)
        except Exception as e:
            print '********  Error while trying to insert data point for race {0}\n{1}'.format(raceActivity.id, e)
            
    def _DataPointExists(self, athlete_id, race_activity_id):
        exists = False

        results = table_service.query_entities(table_name='DataPoints', filter="PartitionKey eq '{0}' and RowKey eq '{1}'".format(athlete_id, race_activity_id))
        
        for point in results:
            exists = True
        
        return exists

    def _DataPointExcluded(self, athlete_id, race_activity_id):
        excluded = False

        results = table_service.query_entities(table_name='ExcludedDataPoints', filter="PartitionKey eq '{0}' and RowKey eq '{1}'".format(athlete_id, race_activity_id))
        
        for point in results:
            excluded = True
        
        return excluded
        
    @staticmethod
    def _data_check_velocity_bins_vs_workout_duration():
        # The sum of the time across the bins for a workout should be close to the workout duration
        # it should never be more than, but may be less than due to data outside of the bins being left out
        binTimeSum = 0
        results = []

        # get the datapoints
        datapoints = table_service.query_entities(table_name='DataPoints', filter=None, select="workout_duration, PartitionKey, RowKey")

        for datapoint in datapoints:
            
            velocity_bins = table_service.get_entity(table_name='VelocityBins', partition_key=datapoint['PartitionKey'], row_key=datapoint['RowKey'] )
            binTimeSum = 0

            for bin in velocity_bins.keys():
                if bin.find('velocity') != -1 :
                    binTimeSum += velocity_bins[bin]
            
            diff = datapoint['workout_duration'] - binTimeSum
            if (datapoint['workout_duration'] > 0):
                var = float(diff) / float(datapoint['workout_duration'])
            flag = True if diff < 0 else False
            results.append([datapoint['PartitionKey'], datapoint['RowKey'], datapoint['workout_duration'], binTimeSum, diff, var, flag ])

            
            np.savetxt("velocity_check.csv", results, '%s', delimiter=',' )
        print "velocity_check.csv"

    @staticmethod
    def ClearDataPoints():
        tables = ['DataPoints', 'VelocityBins', 'CadenceBins', 'AuditDataPointActivities']

        for table in tables:

            rows = table_service.query_entities(table)
            for row in rows:
                table_service.delete_entity(table_name=table, partition_key=row['PartitionKey'], row_key=row['RowKey'] )

    @staticmethod
    def UpdateExclusionList():
        excluded_data_points = table_service.query_entities(table_name='DataPoints', filter='excluded eq true')

        for data_point in excluded_data_points:
            a = { 'PartitionKey':data_point['PartitionKey'], 'RowKey':data_point['RowKey'] }
            table_service.insert_or_replace_entity('ExcludedDataPoints', a )