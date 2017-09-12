#-*- coding:utf-8 -*-
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

import time
import haversine
import umsgpack
import networkx as nx
import pymp
import OSMDatabase
import scipy.spatial as spatial

import Settings
from SharedFunctions import *




def createRailwayLike(mode):    
    # Obtaining stopNodes
    t1=time.time()

    
    stopNodesTemp=getStopNodes(osmd,mode)
    stopNodesTemp=stopNodesTemp-osmd.getAssignedStops([("route","subway"),("route","light_rail"),("route","tram")])
    stopNodesTemp=stopNodesTemp|osmd.getAssignedStops([("route","rail")])
    
    necessaryToKeep=osmd.keepKVMembers(stopNodesTemp,[("railway",["rail"])])
    stopNodesTemp=osmd.filterKVMembers(stopNodesTemp,[("tram",["yes"]),("bus",["yes"]),("light_rail",["yes"]),("subway",["yes"]),("railway",["light_rail"]),("railway",["subway"])])
    stopNodesTemp=stopNodesTemp|necessaryToKeep        

    stopNodesTemp=list(stopNodesTemp)
    stopNodesLocationsT=osmd.getLocations(stopNodesTemp)
    stopNodes=[n for n in stopNodesTemp if (stopNodesLocationsT[n][0]<180 and stopNodesLocationsT[n][0]>-180)]
    stopNodesLocations=[(stopNodesLocationsT[n][0],stopNodesLocationsT[n][1]) for n in stopNodes]
    stopNodesSet=set(stopNodes)

    t2=time.time()
    print("%d stopnodes identified in %0.2fs ..."%(len(stopNodes),t2-t1))

    t1=time.time()
    if len(stopNodesLocations)>0:
        tree = spatial.KDTree(stopNodesLocations)
    t2=time.time()
    print("KD tree over stopnodes created in %0.2fs ..."%(t2-t1))

    # Obtaining relevant ways
    t1=time.time()
    
    relevantWays=set(osmd.getKVs("railway",["rail","train","railway"],1) + osmd.getKVs("route",["rail","railway","train"],1) + osmd.getKVs("train",["yes"],1))
    relevantWays=osmd.filterKVs(relevantWays,[["railway",["station","platform","abandoned","disused","razed"]],["subway",["yes"]],["light_rail",["yes"]],["tram",["yes"]],["bus",["yes"]],["station",["light_rail","subway"]]]) #["service",["spur","siding","yard"]]])
    
    members=osmd.getMembers(relevantWays)
    maxspeeds=osmd.getValues(members.keys(),"maxspeed")
    nodes=set([a for x in members for a in members[x]])
    nodeLocations=osmd.getLocations(nodes)

    allLocations={}
    for a in stopNodesLocationsT:
        allLocations[a]=stopNodesLocationsT[a]
    for a in nodeLocations:
        allLocations[a]=nodeLocations[a]

    t2=time.time()
    print("Relevant ways identified in %0.2fs ..."%(t2-t1))

    # Generating graph
    t1=time.time()
    G=nx.Graph()
    alledges=pymp.shared.dict()
    relevantWaysL=list(relevantWays)
    with pymp.Parallel(10) as p:		
        for l in p.range(0,len(relevantWaysL)):
            MYL=[]
            w=relevantWaysL[l]        
            maxspeed=convert_maxspeed(maxspeeds.get(w,Settings.MAX_SPEED_DEFAULT[mode]),Settings.MAX_SPEED_DEFAULT[mode])
            Lt=extendWay(tree,stopNodes,stopNodesLocations,allLocations,members[w],maxspeed,Settings.MAX_STATION_DISTANCE_THRESHOLD_CONNECTION[mode])        
            for x in range(len(Lt)-1):                
                distance=haversine.haversine((allLocations[Lt[x]][1],allLocations[Lt[x]][0]), (allLocations[Lt[x+1]][1],allLocations[Lt[x+1]][0]))	
                tempdist=(1000*distance)/(float(maxspeed)/3.6)                     
                MYL.append([Lt[x],Lt[x+1],tempdist,float(maxspeed),float(distance)])
            alledges[l]=MYL

    for l in range(0,len(relevantWaysL)): 
        for [a,b,c,d,e] in alledges[l]:
            G.add_edge(a,b,weight=c,ms=d,dist=e)            
        
    t2=time.time()
    print("Created graph in %0.2fs ..."%(t2-t1))

    # Peforming completion
    t1=time.time()
    performCompletion(G,allLocations,Settings.MAX_SPEED_DEFAULT[mode],Settings.MAX_COMPLETION_THRESHOLD[mode])
    t2=time.time()
    print("Performed completion in %0.2fs ..."%(t2-t1))
    

    # Extracting logical network
    t1=time.time()
    finalLinksBeforeMerging=extractStopNodeNeighbors(G,stopNodes,allLocations,True)
    t2=time.time()
    print("Extracting stopNode neighbors in %0.2fs ..."%(t2-t1))

    # Merging stations
    t1=time.time()
    tnodes={}
    for [a,b,dist,tempd] in finalLinksBeforeMerging:            
        tnodes[a]=[allLocations[a][0],allLocations[a][1],""]
        tnodes[b]=[allLocations[b][0],allLocations[b][1],""]    

    names=osmd.getNames(tnodes.keys())
    finalLinks=mergeStations(finalLinksBeforeMerging,stopNodesSet,allLocations,names,{},Settings.MAX_SPEED_DEFAULT[mode],Settings.MAX_STATION_DISTANCE_THRESHOLD[mode],Settings.MAX_STATION_DISTANCE_THRESHOLD_HIGHLYSIMILARNAME,Settings.MAX_STATION_DISTANCE_THRESHOLD_IDENTICALNAME)
    t2=time.time()
    print("Merging stations in %0.2fs ..."%(t2-t1))


    return finalLinks,allLocations,stopNodesSet


def obtainGraph(pars):
    rids,allLocations,mode,stopNodes,stopNodesLocations,stopNodesLocationsT,rid,Settings.MAX_SPEED_DEFAULT[mode],Settings.MAX_STATION_DISTANCE_THRESHOLD_CONNECTION[mode]=pars

    allLocationsCopy=allLocations.copy()
    additionalLocationsColl={}
    partFinalLinksBeforeMergingAll=[]
    for rid in rids:
        partG,additionalLocations=getGraph(osmd,mode,tree,stopNodes,stopNodesLocations,stopNodesLocationsT,rid,Settings.MAX_SPEED_DEFAULT[mode],Settings.MAX_STATION_DISTANCE_THRESHOLD_CONNECTION[mode])
        
        for x in additionalLocations:
            allLocationsCopy[x]=additionalLocations[x]
            additionalLocationsColl[x]=additionalLocations[x]
        
        partFinalLinksBeforeMerging=extractStopNodeNeighbors(partG,stopNodes,allLocationsCopy,False)
        partFinalLinksBeforeMergingAll=partFinalLinksBeforeMergingAll+partFinalLinksBeforeMerging
        
    return partFinalLinksBeforeMergingAll,additionalLocationsColl
    
    


print("************ Doing "+Settings.REGION+" ************")
t1=time.time()
osmd=OSMDatabase.OSMDatabase(Settings.REGION,False)
t2=time.time()
print("OSMD loaded in %0.2fs ..."%(t2-t1))


finalLinks,allLocations,stopNodesSet=createRailwayLike("rail")


nodes={}    
links=[]
seenlinks={}

for [a,b,dist,tempd] in finalLinks:            
    nodes[a]=[allLocations[a][0],allLocations[a][1],""]
    nodes[b]=[allLocations[b][0],allLocations[b][1],""]
    k=set([a,b])            
    if frozenset(k) not in seenlinks:                    
        seenlinks[frozenset(k)]=[]                
    seenlinks[frozenset(k)].append(("1",dist,tempd))

names=osmd.getNames(nodes.keys())
for n in list(nodes.keys()):
    nodes[n]=[nodes[n][0],nodes[n][1],names.get(n,"XXX"),n in stopNodesSet]
    
for l in seenlinks:          
    if list(l)[0]!=list(l)[1]:
        minindex=0
        mintime=seenlinks[l][minindex][2]
        for x in range(len(seenlinks[l])):
            if seenlinks[l][x][2]<mintime:
                minindex=x
                mintime=seenlinks[l][x][2]
            
        links.append([list(l)[0],list(l)[1],seenlinks[l][minindex][1],seenlinks[l][minindex][2]])  

links=list(links)



fnodes=open(Settings.REGION+"_nodes.csv","wt")
fnodes.write("ID,lon,lat,name,station\n")
    
flinks=open(Settings.REGION+"_links.csv","wt")
flinks.write("IDfrom,IDto,distance_km,tempdistance_s\n")

for n in nodes:
    
    name=str(nodes[n][2])
    if name=="XXX":
        name=""
    if len(nodes[n])==4:
        fnodes.write('%s,%f,%f,"%s",%s\n'%(n,nodes[n][0],nodes[n][1],name.replace('"',''),str(nodes[n][3])))
    else:
        fnodes.write("%s,%f,%f,%s%,%s\n"%(n,nodes[n][0],nodes[n][1],name,str(True)))
    
for (a,b,distance,tempd) in links:        
    flinks.write("%s,%s,%f,%f\n"%(a,b,distance,tempd))


fnodes.close()
flinks.close()

