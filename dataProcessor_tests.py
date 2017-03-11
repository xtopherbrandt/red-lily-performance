#!/usr/bin/python

import unittest
from dataProcessor import DataProcessor

class TestDataProcessor( unittest.TestCase ):
    def test_duration_of_points_with_increasing_values(self):
        measurements=[1.0,2.0,3.0]
        times=[0,1,2]
        bins={}
        DataProcessor.binMeasurements( bins, measurements, times, 'test', 1.0, 3.0, 1.0 )
        
        self.assertEqual({'test_1.0-2.0':1, 'test_2.0-3.0':1}, bins )
   
    def test_duration_of_points_with_same_values(self):
        measurements=[2.0,2.0,2.0]
        times=[0,1,2]
        bins={}
        DataProcessor.binMeasurements( bins, measurements, times, 'test', 1.0, 3.0, 1.0 )
        
        self.assertEqual({'test_1.0-2.0':0,'test_2.0-3.0':2}, bins )

    def test_duration_of_points_with_decreasing_values(self):
        measurements=[3.0,2.0,1.0]
        times=[0,1,2]
        bins={}
        DataProcessor.binMeasurements( bins, measurements, times, 'test', 1.0, 3.0, 1.0 )
        
        self.assertEqual({'test_1.0-2.0':1, 'test_2.0-3.0':1}, bins )

    def test_binning_of_many_values_spread_over_many_bins(self):
        '''
        Remember that the measured value is the mediam between the two points.
        So the values recorded are:
            1.65 for 2 --> test_1.5-2.0
            2.75 for 2 --> test_2.5-3.0
            3.85 for 2 --> test_3.5-4.0
            4.95 for 2 --> test_4.5-5.0
            6.05 for 2 --> test_6.0-6.5
            7.15 for 2 --> test_7.0-7.5
            8.25 for 2 --> test_8.0-8.5
            9.35 for 2 --> test_9.0-9.5
        '''
        measurements=[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9]
        times=[0,2,4,6,8,10,12,14,16]
        bins={}
        DataProcessor.binMeasurements( bins, measurements, times, 'test', 1.0, 10.0, 0.5 )
        
        self.assertEqual({'test_1.0-1.5':0, 'test_1.5-2.0':2, 'test_2.0-2.5':0, 'test_2.5-3.0':2, 'test_3.0-3.5':0, 'test_3.5-4.0':2, 'test_4.0-4.5':0, 'test_4.5-5.0':2, 'test_5.0-5.5':0, 'test_5.5-6.0':0, 'test_6.0-6.5':2, 'test_6.5-7.0':0, 'test_7.0-7.5':2, 'test_7.5-8.0':0, 'test_8.0-8.5':2, 'test_8.5-9.0':0, 'test_9.0-9.5':2, 'test_9.5-10.0':0}, bins )
        
if __name__ == '__main__':
    unittest.main()