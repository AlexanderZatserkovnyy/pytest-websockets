import csv
import sys
import math

inputfile='Cycling-flat-file-for-PACE-proj-DQ-factors.csv'
outputfile='Cycling-flat-ucl-outliers.csv'

thekeys = [ 'category_name','class_name','brand_name','subclass_name' ]
time_name = 'monthdate' 
factor_name = 'proj.factor1'

Ko = 2
#############################################################
csv1 = csv.reader(open(inputfile, 'r'))

fieldnames=[]
resdict = {}
for row in csv1:
     if len(fieldnames)==0:
        fieldnames=[ r.strip() for r in row ]
        thekeys_fnums = [ fieldnames.index(x.strip()) for x in thekeys ]
        time_fnum = fieldnames.index(time_name.strip())
        factor_fnum = fieldnames.index(factor_name.strip())
     else:
        key = tuple([ row[i] for i in thekeys_fnums ])
        thetime = row[time_fnum]
        factor = float(row[factor_fnum])
        if factor==0:
            continue
        if key not in resdict:
            resdict[key] = ( [(thetime,factor)], [0, 0, 0, 0] )
        else:
            resdict[key][0].append((thetime,factor))

for k in resdict.keys():
    L = len(resdict[k][0])
    resdict[k][1][0] = sum([ v[1] for v in resdict[k][0] ])/L #mean
    resdict[k][1][1] = math.sqrt( abs(sum([ v[1]**2 for v in resdict[k][0] ])/L - resdict[k][1][0]**2) ) # stddev
    resdict[k][1][2] = resdict[k][1][0] + Ko*resdict[k][1][1] # UCL
    resdict[k][1][3] = resdict[k][1][0] - Ko*resdict[k][1][1] # LCL 
    for v in resdict[k][0][:]:
        if v[1]<=resdict[k][1][2]: # filter non outliers
            resdict[k][0].remove(v)
    if len(resdict[k][0])==0:
        del resdict[k]

### output to csv
header_names=thekeys+[ time_name, factor_name ]
fdata = open(outputfile,'w')
fdata.write(reduce( lambda x, y: x+','+y, header_names )+'\n')
for k in sorted(resdict.keys()):
    for t,f in sorted(resdict[k][0]):
        lst = list(k)+[t,str(round(f,2))]
        fdata.write(reduce(lambda x, y: x+','+y,lst)+'\n')

