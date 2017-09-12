import haversine
import math
import string
import pymp
import editdistance
import scipy.spatial as spatial
from collections import deque
import networkx as nx
import time
import numpy as np




def getStopNodes(osmd,mode):
    result=set()    
    
    stopN=osmd.getKVs("railway",["stop","station","halt","stop_position"],0)        
    stopW=osmd.getKVs("railway",["stop","station","halt","stop_position"],1)        
    stopR=osmd.getKVs("railway",["stop","station","halt","stop_position"],2)
            
    stopAreasW=osmd.getKVs("site",["stop_area","stop_area_group"],1)
    stopAreasR=osmd.getKVs("site",["stop_area","stop_area_group"],2)            
    
    stopRmembers=osmd.getMembers(stopR+stopAreasR)        
    for r in stopRmembers:
        for x in stopRmembers[r]:                
            if x[0]=="W":
                stopAreasW.append(x)
            elif x[0]=="N":
                stopN.append(x)        
    
    stopWmembers=osmd.getMembers(stopW+stopAreasW)        
    for w in stopWmembers:
        for x in stopWmembers[w]:
            stopN.append(x)
            
    candidates=set(stopN+stopW+stopR+stopAreasW+stopAreasR)
    result=osmd.filterKVs(candidates,[["railway",["abandoned","disused"]],["subway",["yes"]],["light_rail",["yes"]],["tram",["yes"]],["bus",["yes"]],["station",["light_rail","subway"]]])
    
    return result

def getPointToLineDistGetPoint(x1,y1, x2,y2, x3,y3):
    px = x2-x1
    py = y2-y1
    something = px*px + py*py
    
    if (float(something)==0):
        return (x3-x1,y3-y1)

    u =  ((x3 - x1) * px + (y3 - y1) * py) / float(something)
    if u > 1:
        u = 1
    elif u < 0:
        u = 0
    x = x1 + u * px
    y = y1 + u * py
    return (x,y)


def convert_maxspeed(inmaxspeed,defaultmaxspeed):
    result=""
        
    try:
        result=float(inmaxspeed)
    except ValueError:
        if "mph" in inmaxspeed:
            t=inmaxspeed
            t=string.replace(t,"mph","")    
            t=string.replace(t," ","")    
            try:
                result=float(t)*1.60934
            except ValueError:
                result=defaultmaxspeed

        elif "kph" in inmaxspeed:
            t=inmaxspeed
            t=string.replace(t,"kph","")    
            t=string.replace(t," ","")    
            try:
                result=float(t)
            except ValueError:
                result=defaultmaxspeed        
        
        elif inmaxspeed=="":
            result=defaultmaxspeed        
            
        else:
            result=defaultmaxspeed        
        
    
    if result<0.01:
        result=0.01        
            
    return result

def extendWay(tree,stopNodes,stopNodesLocations,nodeLocations,members,maxspeed,msdtc):
    bestDistances={}
    deltas={}
    
    for l in range(0,len(members)-1):
        start=members[l]
        startx,starty=nodeLocations[start][0],nodeLocations[start][1]
        end=members[l+1]
        endx,endy=nodeLocations[end][0],nodeLocations[end][1]
        
        if startx==startx and endx==endx:
            hav=haversine.haversine((starty,startx),(endy,endx))
            dist=math.sqrt((endx-startx)*(endx-startx) + (endy-starty)*(endy-starty))
            delta=int(math.ceil(dist/0.005))
            deltas[l]=delta
            for step in range(delta):
                curx=float(endx-startx)*step/delta+startx
                cury=float(endy-starty)*step/delta+starty
                
                nextx=float(endx-startx)*(step+1)/delta+startx
                nexty=float(endy-starty)*(step+1)/delta+starty            
                res=tree.query((curx,cury),3)
                for r in res[1]:
                    
                    inside=stopNodes[r] in members
                    compatible=True
                    if (not inside) and compatible:                                        
                        statlinex,statliney=getPointToLineDistGetPoint(curx,cury, nextx,nexty, stopNodesLocations[r][0],stopNodesLocations[r][1])
                        havcurstat=haversine.haversine((stopNodesLocations[r][1],stopNodesLocations[r][0]),(statliney,statlinex))                    
                        L=bestDistances.get(stopNodes[r],["",999999,-1,-1,-1,-1])
                        
                        if L[1]>havcurstat and havcurstat<msdtc:
                            bestDistances[stopNodes[r]]=[stopNodes[r],havcurstat,l,l+1,step,delta]
    
    revbest={}
    for x in bestDistances:
        if bestDistances[x][2] not in revbest:
            revbest[bestDistances[x][2]]={}
        if bestDistances[x][4] not in revbest[bestDistances[x][2]]:
            revbest[bestDistances[x][2]][bestDistances[x][4]]=[]            
        revbest[bestDistances[x][2]][bestDistances[x][4]].append(bestDistances[x])
    
    updatedMembers=[]
    for l in range(0,len(members)-1):            
        updatedMembers.append(members[l])
        if l in revbest:
            delta=deltas[l]
            for step in range(delta):          
                if step in revbest[l]:
                    for tempx in revbest[l][step]:
                        updatedMembers.append(tempx[0])                        
                    #print("Adding:",revbest[l][step][0])
    if len(members)>0:
        updatedMembers.append(members[-1])
    return updatedMembers

def performCompletion(G,allLocations,_maxspeed,distthreshold):
    deg=G.degree()        
    nlist=[n for n in deg if deg[n]==1 and allLocations[n][0]==allLocations[n][0]]    
    nlistLocations=[(allLocations[n][0],allLocations[n][1]) for n in nlist]
    treeCompletion = spatial.KDTree(nlistLocations)
    
    XTODO=list(G.edges())
    
    tempresults=pymp.shared.dict()
    with pymp.Parallel(10) as p:		
        for l in p.range(0,len(XTODO)): 
            MYL=[]
            (a,b)=XTODO[l]
            if len(tempresults)%10000==0:
                print("Progress: %d/%d"%(len(tempresults),len(XTODO)))
            res=treeCompletion.query((allLocations[a][0],allLocations[a][1]),5)
            res2=treeCompletion.query((allLocations[b][0],allLocations[b][1]),5)
            for r in set(res[1] | res2[1]):                                
                if r < len(nlist):
                    othern=nlist[r]        
                    px,py=getPointToLineDistGetPoint(allLocations[a][0],allLocations[a][1],allLocations[b][0],allLocations[b][1],allLocations[othern][0],allLocations[othern][1])                    
                    distance=haversine.haversine((allLocations[othern][1],allLocations[othern][0]), (py,px))
                        
                    if distance<distthreshold:
                        if not G.has_edge(a,othern):
                            d2=haversine.haversine((allLocations[a][1],allLocations[a][0]), (allLocations[othern][1],allLocations[othern][0]))	                        
                            tempdist2=(1000*d2)/(float(_maxspeed)/3.6)                
                            MYL.append([a,othern,tempdist2,float(_maxspeed),float(d2)])
                                
                        if not G.has_edge(b,othern):
                            d2=haversine.haversine((allLocations[b][1],allLocations[b][0]), (allLocations[othern][1],allLocations[othern][0]))	                        
                            tempdist2=(1000*d2)/(float(_maxspeed)/3.6)                
                            MYL.append([b,othern,tempdist2,float(_maxspeed),float(d2)])                                             
                            
            tempresults[l]=MYL
            
    for l in range(0,len(XTODO)):                
        if l in tempresults:        
            if len(tempresults[l])>0:
                for [a,b,c,d,e] in tempresults[l]:                
                    G.add_edge(a,b,weight=c,ms=d,dist=e)            
        
    print(len(G.edges())) 

def extractStopNodeNeighbors(parG,stopNodes,allLocations,printStatus=False):
    
    G=parG.copy()
    
    finalLinks=[]        
    if len(list(G.edges()))==0:
        return finalLinks        
    
    allNodes=set(G.nodes())
    curProcessed=0

    deg=G.degree()        
    todo=[sn for sn in stopNodes if sn in allNodes] +[n for n in deg if deg[n]>2]    
    
    totoSet=set(todo)
    
    if len(todo)==0:
        return finalLinks
                
    neighbors = G.neighbors_iter
    
    timestart=time.time()
    
    toBeRemoved=[]        
    
    for sn in todo:
        
        TIME1=time.time()        
        visited = set([sn])
        visitedLinks=[]
        XS=set()
        queue = deque([(sn, neighbors(sn))])
        XS.add(sn)
        
        Gsub=nx.Graph()                    
        
        while queue:
            parent, children = queue[0]
            try:
                child = next(children)
                
                Gsub.add_edge(parent,child,weight=G[parent][child]["weight"],ms=G[parent][child]["ms"],dist=G[parent][child]["dist"])
                          
                if child not in visited:                    
                    if child in totoSet and child!=sn and not math.isnan(allLocations[sn][0]) and not math.isnan(allLocations[child][0]):                                            
                        XS.add(child)
                    else:                        
                        queue.append((child, neighbors(child)))                        
                        
                    visited.add(child)
            
            except StopIteration:
                queue.popleft()
        
        TIME2=time.time()        
        if (len(visited)>50 or curProcessed%1000==0) and printStatus:
            print(curProcessed,len(todo),len(XS),len(visited))
        
        for x in XS: 
            #print("  ",x)
            if x in Gsub.nodes():
                allSP=nx.shortest_path(Gsub,x)                                                        
                for y in XS:     
                    if y in Gsub.nodes():
                        if x!=y:
                            SP=allSP[y]
                            valid=True
                            for tempn in SP[1:-1]:
                                if tempn in totoSet:
                                    valid=False
                            
                            if valid:
                                SPweight=0.0
                                distsum=0.0
                                
                                for l in range(len(SP)-1):
                                    SPweight+=G[SP[l]][SP[l+1]]["weight"]
                                    distsum+=G[SP[l]][SP[l+1]]["dist"]
                                    
                                finalLinks.append([x,y,distsum,SPweight])   

        TIME3=time.time()        
        for (x1,x2) in Gsub.edges():            
            G.remove_edge(x1,x2)
        TIME4=time.time()        
        curProcessed=curProcessed+1
                            
    return finalLinks

def mergeStations(allinks,stopNodesSet,allLocations,names,sa2mem,maxspeed,MAX_STATION_DISTANCE_THRESHOLD,MAX_STATION_DISTANCE_THRESHOLD_HIGHLYSIMILARNAME,MAX_STATION_DISTANCE_THRESHOLD_IDENTICALNAME):
    def shouldChangeOrder(n1,n2):
        if names[n1]=="XXX" and names[n2]!="XXX":
            return True            
        return n1>n2
    
    allnodesdict={}
        
    for [a,b,dist,timed] in allinks:
        for n in [a,b]:       
            loc=allLocations[n]
            if (not math.isnan(loc[0])) and (not math.isnan(loc[1])):
                allnodesdict[n]=(loc[0],loc[1])                            
                
                    
    nodes=sorted(list(allnodesdict.keys()))    
    
    locs=[allnodesdict[n] for n in nodes]
        
    repr2child={}
    child2repr={}
    
    
    stationComponmentG=nx.Graph()
    nonstationComponmentG=nx.Graph()
    
    for n in nodes:
        stationComponmentG.add_node(n)
    
    if len(locs)>0:        
        tree = spatial.KDTree(locs) 
        for n in nodes:
            loc=allnodesdict[n]                        
            res=tree.query((loc[0],loc[1]),40)
            for x in res[1]:
                if x<len(nodes) and x!=n:
                    n2=nodes[x]
                    distance=haversine.haversine((loc[1],loc[0]), (allnodesdict[n2][1],allnodesdict[n2][0]))	
                                            
                    ed=editdistance.eval(names[n],names[n2])                    
                    reled=float(ed)/min(len(names[n]),len(names[n2]))
                    
                    if distance<MAX_STATION_DISTANCE_THRESHOLD or (distance<MAX_STATION_DISTANCE_THRESHOLD_HIGHLYSIMILARNAME and reled<0.2 and (names[n]!="XXX" or names[n2]!="XXX")) or (distance<MAX_STATION_DISTANCE_THRESHOLD_IDENTICALNAME and reled==0 and names[n]!="XXX"):
                        
                        if n in stopNodesSet and n2 in stopNodesSet:
                            stationComponmentG.add_edge(n,n2)
                            
    for sa in sa2mem:
        for p in range(len(sa2mem[sa])-1):
            stationComponmentG.add_edge(sa2mem[sa][p],sa2mem[sa][p+1])
    
        
    def selectParent(nlist,allLocations):
        for n in nlist:
            if names.get(n,"XXX")!="XXX" and n in allLocations:
                return n
      
        for n in nlist:
            if n in allLocations:
                return n
      
        return list(nlist)[0]
        
    components = nx.connected_components(stationComponmentG)
    for nodes in components:
        parent=selectParent(nodes,allLocations)
        for n in nodes:
            child2repr[n]=parent
    
    
    linksDictNew=[]
    
    for [a,b,dist,timed] in allinks:
        a1=child2repr.get(a,a)
        b1=child2repr.get(b,b)
        allLocations
        if a1 in allLocations:
            loca1=(allLocations[a1][1],allLocations[a1][0])
        else:
            loca1=(allLocations[a][1],allLocations[a][0])
            
        if b1 in allLocations:
            locb1=(allLocations[b1][1],allLocations[b1][0])
        else:
            locb1=(allLocations[b][1],allLocations[b][0])
        
        if a1!=b1:
            distanceNew=haversine.haversine(loca1, locb1)	
            if dist!=0:
                linksDictNew.append([a1,b1,distanceNew,timed*distanceNew/dist])
            else:
                tempdist2=(1000*distanceNew)/(float(maxspeed)/3.6)                
                linksDictNew.append([a1,b1,distanceNew,tempdist2])

    return linksDictNew

def getLongestPair(Gx):
    sn=list(Gx.nodes())[0]
    d1=nx.shortest_path(Gx,sn)
    start=sn
    startlen=0
    for t in d1:
        if len(d1[t])>startlen:
            start=t
            startlen=len(d1[t])
    
    d2=nx.shortest_path(Gx,start)
    end=sn
    endlen=0
    best=[]
    for t in d1:
        if len(d2[t])>endlen:
            end=t
            endlen=len(d2[t])
            best=d2[t]
                
    return best



def getGraph(osmd,mode,tree,stopNodes,stopNodesLocations,stopNodesLocationsT,rid,maxs,msdtc):
    
    
    rmembers=osmd.getRelationMembers([rid])
    openW=[]
    for [erole, eref] in rmembers[rid]:        
        if eref[0]=="W" and erole!="stop" and isinstance(erole, basestring) and erole[-4:]!="stop" and erole!="platform":                
            openW.append(eref)
    
    
    wmembers=osmd.getMembers(openW)
    maxspeeds=osmd.getValues(wmembers.keys(),"maxspeed")    
    
    
    nodes=set([a for x in wmembers for a in wmembers[x]])
    additionalLocations=osmd.getLocations(nodes)

    allLocations={}
    for x in stopNodes:        
        allLocations[x]=stopNodesLocationsT[x]
        
    for x in additionalLocations:
        allLocations[x]=additionalLocations[x]
            
    G=nx.Graph()
    MYL=[]
    for w in wmembers:          
        maxspeed=convert_maxspeed(maxspeeds.get(w,maxs),maxs)        
        Lt=extendWay(tree,stopNodes,stopNodesLocations,allLocations,wmembers[w],maxspeed,msdtc)
        
        for x in range(len(Lt)-1):                
            distance=haversine.haversine((allLocations[Lt[x]][1],allLocations[Lt[x]][0]), (allLocations[Lt[x+1]][1],allLocations[Lt[x+1]][0]))	
            tempdist=(1000*distance)/(float(maxspeed)/3.6)                     
            MYL.append([Lt[x],Lt[x+1],tempdist,float(maxspeed),float(distance)])
    
    for [a,b,c,d,e] in MYL:
        G.add_edge(a,b,weight=c,ms=d,dist=e)            
        
    # Connect ring-like lines
    CCs=sorted(nx.connected_components(G), key = len, reverse=False)
    for cc in CCs:
        Gx=G.subgraph(cc)        
        P=getLongestPair(Gx)
        totaldistance=0.0
        for l in range(len(P)-1):
            pos1=(allLocations[P[l]][1],allLocations[P[l]][0])
            pos2=(allLocations[P[l+1]][1],allLocations[P[l+1]][0])
            distance=haversine.haversine(pos1,pos2)	
            totaldistance+=distance
                                
        pos1=(allLocations[P[0]][1],allLocations[P[0]][0])
        pos2=(allLocations[P[-1]][1],allLocations[P[-1]][0])
                    
        startenddist=haversine.haversine(pos1,pos2)        
        if startenddist*50 < totaldistance:                
            mss=[]
            for l in range(len(P)-1):
                mss.append(G[P[l]][P[l+1]]["ms"])
            meanms=float(np.mean(mss))
            tempdist=(1000*startenddist)/(meanms/3.6)                
            G.add_edge(P[0],P[-1],weight=tempdist,ms=meanms,dist=float(startenddist))
               
               
    return G,additionalLocations
    

