from __future__ import print_function
import math
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import ConfigParser

sc = SparkContext(appName="SPM-Outliers")
############# Set inputs from the command line and the conf file ######################
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: SPM-Outliers.py <conf file> <csv input table> <csv output table>", file=sys.stderr)
        exit(-1)

#Parse conf file
config = ConfigParser.RawConfigParser()
config.read(sys.argv[1])
# Set outlier coefficient 
K=config.getint('Common', 'K') # is it test dependant?
# Spaces in field names have meaning! Some table names have spaces 
# Select field roles in the tests
TimeField=config.get('Common', 'TimeField').strip('\'\"')
FactorField=config.get('Common', 'FactorField').strip('\'\"')
#IdField=config.get('Common', 'IdField').strip('\'\"')
Tests=config.sections()
Tests.remove('Common')
# get array of different parameter sets for the different experiments in the set
# e.g. (Channel,Region,Class), (Channel,Region), (Channel,Class)  ...
TestParamSets=[]
for t in Tests:
    TestParamSets.append(config.get(t,'Params').strip('\'\"'))

csvInput=sys.argv[2]
csvOutput=sys.argv[3]
#################################################
sqlContext = SQLContext(sc)
# 
df = ( sqlContext.read.format("com.databricks.spark.csv").
                             option("header", "true").option("inferschema", "true").
                             option("mode", "DROPMALFORMED").load(csvInput) )
# 
df.cache()

#sch1 = df.schema
#sch2 = StructType( [ StructField('Mean',DoubleType()),StructField('StdDev',DoubleType()),StructField('UCL',DoubleType()),StructField('LCL',DoubleType()) ] )

csvFieldNames = [ f.strip() for f in df.columns ]
TimeNum = csvFieldNames.index(TimeField.strip())
FactorNum = csvFieldNames.index(FactorField.strip())
#IdNum = csvFieldNames.index(IdField.strip())
testCounter = 1 # for the output naming
###################################################################################
# looks Outliers for every parameter set in TestParamSets
for pars in TestParamSets:  # loop via tests
    arParams = [ x.strip() for x in pars.split(',') ] # select a test parameters field names
    ParamsNumArr = [ csvFieldNames.index(x.strip()) for x in arParams ] # look the field numbers in the data frame (in the original table)
    #Remap the data to the form ((Channel,Region,Class), [ ppmonth[0], ppmonth[1] ....  ], [ Factor[0], Factor[1], ... ] )
    # filter rows with bad Factor values (non float numbers)

    rdd_remap = df.rdd.filter(lambda row: isinstance( row[FactorNum],float ) ). \
                       map(lambda row: (tuple([ row[x] for x in ParamsNumArr ]),[ row[TimeNum],row[FactorNum] ])).groupByKey()
    # Add mean and stddev for every (Channel,Region,Class) F(t)
    # add mean 
    rdd_MD =  rdd_remap.map(lambda (k,v): (k,sum([f[1] for f in v])/len(v),v)). \
                        map(lambda (k,m,v): (k,m,math.sqrt(sum([(f[1])**2 for f in v])/len(v) - m**2),v)) # add stddev using mean
    # drop non outliers from (t,f,dsid) arrays
    rdd_Outliers = rdd_MD.map( lambda (k,m,d,v): (k,m,d,m+K*d,m-K*d,4,[ x for x in v if (x[1]>m+K*d) or (x[1]<m-K*d) ]) ). \
                          filter(lambda (k,m,d,u,l,v): len(v)>0) # drop (Channel,Region,Class) without outliers
    # separate (test key, mean, stddev) to use later with flat (test key, time, factor, id )
    rdd_OutliersMD = rdd_Outliers.map(lambda (k,m,d,u,l,v):(k,[round(m,2),round(d,2),round(u,2),round(l,2)]))
    rdd_OutliersV = rdd_Outliers.map(lambda (k,m,d,u,l,v):(k,v))

    # Select field names used in the Test                      
    arrFields = ["retailer"]+arParams+[ TimeField.strip(), FactorField.strip() ]+['Mean','StdDev','UCL', 'LCL']
    #sch = [ sch1[i] for i in ParamsNumArr ] + [ sch1[TimeNum], sch1[FactorNum], sch2[0], sch2[1], sch2[2], sch2[3] ]
    # Format results to one string on a outlier for the Test
    dfResOutliers =  rdd_OutliersV.flatMapValues(lambda v:v).leftOuterJoin(rdd_OutliersMD). \
                                   map(lambda (v,vv): (['']+list(v)+vv[0]+vv[1])).toDF(arrFields)

    # Print full outlier list for the Test
    #make the output to the directory where the source csv , the last number in the name - Test number
    dfResOutliers.replace("","empty").coalesce(1).write.format('com.databricks.spark.csv').options(header='true',delimiter='|').save('tmp-csv-09a0564')
    if testCounter>1:
            csvOut = csvOutput.replace('.csv',str(testCounter)+'.csv')
    else:
            csvOut = csvOutput
    os.system("mv -f tmp-csv-09a0564/p* " + csvOut)
    os.system("rm -rf tmp-csv-09a0564")
    #dfResOutliers.show(10000, truncate=False)
    testCounter +=1  
