# -*- coding: utf-8 -*-
#from __future__ import unicode_literals

#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')


#sqlite3 berlin.data "select count(*) from nodes;"

import math
import time
import os
import numpy as np
import osmium as o
import umsgpack
import json
import chardet
import sqlite3
import string

def convert2Dict(tags):
    res={}
    for x in tags:        
        if x.k in set(["highway","public_transport","railway","amenity","highway","tram","bus","site","light_rail","train","subway","maxspeed","route","construction","station","service"]) or x.k[:4]=="name":
            if x.k!="highway" or (x.k=="highway" and str(x.v) in ["bus_stop","stop","stop_position","yes","no"]):
                res[x.k.replace("'","")]=str(x.v).replace("'","")
    return res

def getNameFromTags(tags):
        name="XXX"        
        if "name" in tags:
            name=tags["name"]
        
        if name=="XXX":
            if "name:en" in tags:
                name=tags["name:en"]        
            
        return name.replace("'","")            


def convert_maxspeed(inmaxspeed):
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
                result=""

        if "kph" in inmaxspeed:
            t=inmaxspeed
            t=string.replace(t,"kph","")    
            t=string.replace(t," ","")    
            try:
                result=float(t)
            except ValueError:
                result=""
        
    return result



            
class OSMDatabase(o.SimpleHandler):
    def __init__(self,region,recreate,pathoverride=""):
        o.SimpleHandler.__init__(self)        
        #self.N={}
        #self.W={}        
        #self.R={}                
        
        #self.membersW={}
        #self.membersR={}
                
        osmfilepath="/work/code/WorldTransportMap/OSM/"+region+"-latest-transport.osm.pbf"
        path="/work/code/WorldTransportMap/REGIONRAWDATA/"+region        
        
        if len(pathoverride)>0:
            path=pathoverride
                
        print(path)
        if not os.path.exists(path+".data") or recreate:
            if os.path.exists(path+".data"):
                os.remove(path+".data")
                
            self.conn = sqlite3.connect(path+".data")
            self.cur = self.conn.cursor()
            
            
            self.cur.execute("CREATE TABLE nodes ('id' int, 'name' string, 'lon' float, 'lat' float);")
            self.cur.execute("CREATE TABLE ways ('id' int, 'name' string);")
            self.cur.execute("CREATE TABLE relations ('id' int, 'name' string);")
            
            self.cur.execute("CREATE TABLE waymembers ('id' int, 'type' int, 'child' int);")
            self.cur.execute("CREATE TABLE relationmembers ('id' int, 'type' int, 'role' string, 'child' int);")
            
            self.cur.execute("CREATE TABLE nodetags ('id' int, 'k' string, 'v' string);")
            self.cur.execute("CREATE TABLE waytags ('id' int, 'k' string, 'v' string);")
            self.cur.execute("CREATE TABLE relationtags ('id' int, 'k' string, 'v' string);")
            
            self.cur.execute("PRAGMA synchronous=FULL") 
        
            
            
            self.apply_file(osmfilepath)  
            self.conn.commit()
            
            self.cur.execute("CREATE INDEX idx_relmembers ON relationmembers(id);")
            self.cur.execute("CREATE INDEX idx_waymembers ON waymembers(id);") 
            self.cur.execute("CREATE INDEX idx_waytags ON waytags(id);") 
            self.cur.execute("CREATE INDEX idx_nodes ON nodes(id);") 
            self.conn.commit()
            
                        
        else:
            self.conn = sqlite3.connect(path+".data")
            self.cur = self.conn.cursor()
            
    def node(self, n):   
        
        T="INSERT INTO nodes (id,name,lon,lat) VALUES ("+str(n.id)+",'"+getNameFromTags(n.tags)+"',"+str(n.location.lon)+","+str(n.location.lat)+");"        
        try:
            self.cur.execute(T)        
        except sqlite3.OperationalError:
            print("ErrorN:",T)
        
        tags=convert2Dict(n.tags)
        for a in tags:        
            try:
                T="INSERT INTO nodetags (id,k,v) VALUES ("+str(n.id)+",'"+str(a)+"','"+str(tags[a])+"');"
                self.cur.execute(T)        
            except sqlite3.OperationalError:
                print("ErrorN2:",T)

    def way(self, w):
        try:
            T="INSERT INTO ways (id,name) VALUES ("+str(w.id)+",'"+getNameFromTags(w.tags)+"');"
            self.cur.execute(T)        
        except sqlite3.OperationalError:
            print("ErrorW:",T)
        
        tags=convert2Dict(w.tags)
        for a in tags:            
            try:
                T="INSERT INTO waytags (id,k,v) VALUES ("+str(w.id)+",'"+str(a)+"','"+str(tags[a])+"');"
                self.cur.execute(T)        
            except sqlite3.OperationalError:
                print("ErrorW2:",T)
        
        for e in w.nodes:            
            try:
                T="INSERT INTO waymembers (id,type,child) VALUES ("+str(w.id)+",0,"+str(e.ref)+");"
                self.cur.execute(T)
            except sqlite3.OperationalError:
                print("ErrorW3:",T)
        
                            
    def relation(self, r):
        try:
            T="INSERT INTO relations (id,name) VALUES ("+str(r.id)+",'"+getNameFromTags(r.tags)+"');"
            self.cur.execute(T)        
        except sqlite3.OperationalError:
            print("ErrorR:",T)
        
        tags=convert2Dict(r.tags)
        for a in tags:      
            try:
                T="INSERT INTO relationtags (id,k,v) VALUES ("+str(r.id)+",'"+str(a)+"','"+str(tags[a])+"');"
                self.cur.execute(T)        
            except sqlite3.OperationalError:
                print("ErrorR2:",T)

        for e in r.members:
            try:
                if e.type=="n":                    
                    self.cur.execute("INSERT INTO relationmembers (id,type,role,child) VALUES ("+str(r.id)+",0,'"+e.role+"',"+str(e.ref)+");")
                elif e.type=="w":                
                    self.cur.execute("INSERT INTO relationmembers (id,type,role,child) VALUES ("+str(r.id)+",1,'"+e.role+"',"+str(e.ref)+");")
                elif e.type=="r":                
                    self.cur.execute("INSERT INTO relationmembers (id,type,role,child) VALUES ("+str(r.id)+",2,'"+e.role+"',"+str(e.ref)+");")
            except sqlite3.OperationalError:
                print("ErrorR3:",T)

    #def _getLocation(self,_id,seen,depth):                
        #if _id[0]=="N":
            #if _id in self.N:
                #return [self.N[_id][1],self.N[_id][2],self.N[_id][1],self.N[_id][2],self.N[_id][1],self.N[_id][2]]
            #else:
                #return [float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
            
        #elif _id[0]=="W":            
            #lon=[]
            #lat=[]
            #if _id in self.W:
                #for X in self.W[_id][1]:                    
                    #lon.append(self.N[X][1])
                    #lat.append(self.N[X][2])
                #return [np.mean(lon),np.mean(lat),min(lon),min(lat),max(lon),max(lat)]
            #else:
                #return [float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
            
        #elif _id[0]=="R":            
            #lon=[]
            #lat=[]
            #if _id in self.R:                                
                #for [mrole,mref] in self.R[_id ][1]:                    
                    
                    #if _id not in seen:                                        
                        #if depth<10:
                            #[x,y,x2,y2,x3,y3]=self._getLocation(mref,seen|set(_id),depth+1)   
                        
                            #if not math.isnan(x):
                                #lon.append(x)                        
                            #if not math.isnan(y):
                                #lat.append(y)            
                                                        
                #if len(lon)>0:
                    #return [np.mean(lon),np.mean(lat),min(lon),min(lat),max(lon),max(lat)]
            
            #return [float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
            
        #else:
            #print("Unknown ID type in OSMDataExtractor, getLocation: ",_id)
            #f=1/0
            #exit(0)
            
    #def getLocation(self,_id):
        #return self._getLocation(_id,set(),0)    
            
    #def getName(self,_id):
        #if _id[0]=="N":
            #return self.N.get(_id,["NONE"])[0]
        #elif _id[0]=="W":
            #return self.W.get(_id,["NONE"])[0]
        #elif _id[0]=="R":
            #return self.R.get(_id,["NONE"])[0]
        
        #return "???"

    def _getRealIDs(self, ids):
        N,W,R=[],[],[]
        for _id in ids:
            if _id[0]=="N":
                N.append(int(_id[1:]))
            
            if _id[0]=="W":
                W.append(int(_id[1:]))
            
            if _id[0]=="R":
                R.append(int(_id[1:]))
        return N,W,R

    def getNames(self, ids):
        res={}
        N,W,R=self._getRealIDs(ids)
        
        self.cur.execute("SELECT id,name FROM nodes WHERE id in ("+",".join([str(id) for id in N])+");")        
        for row in self.cur.fetchall():
            res["N"+str(row[0])]=str(row[1])
        
        self.cur.execute("SELECT id,name FROM ways WHERE id in ("+",".join([str(id) for id in W])+");")        
        for row in self.cur.fetchall():
            res["W"+str(row[0])]=str(row[1])
            
        self.cur.execute("SELECT id,name FROM relations WHERE id in ("+",".join([str(id) for id in R])+");")        
        for row in self.cur.fetchall():
            res["R"+str(row[0])]=str(row[1])
        
        
        for _id in ids:
            if _id not in res:
                res[id]="XXX"
        
        return res
    
    def _getRealNodeLocations(self, realids):
        res={}
        #N,W,R=self._getRealIDs(ids)
        
        self.cur.execute("SELECT id,lon,lat FROM nodes WHERE id in ("+",".join([str(id) for id in realids])+");")        
        for row in self.cur.fetchall():
            res[row[0]]=(row[1],row[2])
        
        return res
    
    def _getWayMembers(self, realids):
        res={}
        self.cur.execute("SELECT id,type,child FROM waymembers WHERE id in ("+",".join([str(id) for id in realids])+");")        
        for row in self.cur.fetchall():            
            if row[0] not in res:
                res[row[0]]=[]
            if row[1]==0:
                res[row[0]].append(row[2])            
        
        return res
    
    def _getRelationMembers(self, realids):
        res={}
        self.cur.execute("SELECT id,type,role,child FROM relationmembers WHERE id in ("+",".join([str(id) for id in realids])+");")        
        for row in self.cur.fetchall():            
            if row[0] not in res:
                res[row[0]]=[]
            
            res[row[0]].append([row[1],row[2],row[3]])            
        
        return res
    
    
    def getRelationMembers(self, ids):
        N,W,R=self._getRealIDs(ids)
        res={}
        T=self._getRelationMembers(R)
        for tid in T:
            if "R"+str(tid) not in res:
                res["R"+str(tid)]=[]            
            for (xtype,xrole,x) in T[tid]:
                if xtype==0:
                    res["R"+str(tid)].append([xrole,"N"+str(x)])
                if xtype==1:
                    res["R"+str(tid)].append([xrole,"W"+str(x)])            
                if xtype==2:
                    res["R"+str(tid)].append([xrole,"R"+str(x)])            
                    
                    
        for _id in ids:
            if _id not in res:
                res[_id]=[]
        
        return res
    
    def getMembers(self, ids):
        res={}
        N,W,R=self._getRealIDs(ids)
        
        T=self._getWayMembers(W)
        for tid in T:
            if "W"+str(tid) not in res:
                res["W"+str(tid)]=[]            
            for x in T[tid]:
                res["W"+str(tid)].append("N"+str(x))            
        
        T=self._getRelationMembers(R)
        for tid in T:
            if "R"+str(tid) not in res:
                res["R"+str(tid)]=[]            
            for (xtype,xrole,x) in T[tid]:
                if xtype==0:
                    res["R"+str(tid)].append("N"+str(x))
                if xtype==1:
                    res["R"+str(tid)].append("W"+str(x))            
                if xtype==2:
                    res["R"+str(tid)].append("R"+str(x))            
        
        for _id in ids:
            if _id not in res:
                res[_id]=[]
        
        return res
    
    
    
    def getLocations(self, ids):
        res={}
        N,W,R=self._getRealIDs(ids)
        
        self.cur.execute("SELECT id,lon,lat FROM nodes WHERE id in ("+",".join([str(id) for id in N])+");")        
        for row in self.cur.fetchall():
            res["N"+str(row[0])]=(row[1],row[2])
        
        allmembersW=self._getWayMembers(W)      
        allnodes=[x for w in allmembersW for x in allmembersW[w]]        
        
        rnl=self._getRealNodeLocations(allnodes)        
        
        for w in W:
            if w in allmembersW:
                curm=allmembersW[w]                                    
                lons=[rnl[x][0] for x in allmembersW[w] if x in rnl]
                lats=[rnl[x][1] for x in allmembersW[w] if x in rnl]
                if len(lons)>0:
                    res["W"+str(w)]=[np.mean(lons),np.mean(lats),min(lons),min(lats),max(lons),max(lats)]
                else:
                    res["W"+str(w)]=[float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
        
        
        allmembersR=self._getRelationMembers(R)
        allnodes=[x for r in allmembersR for (xtype,xrole,x) in allmembersR[r] if xtype==0]        
        rnl=self._getRealNodeLocations(allnodes)
        
                
        allways=[x for r in allmembersR for (xtype,xrole,x) in allmembersR[r] if xtype==1]        
        allmembersW2=self._getWayMembers(allways)      
        allnodesforw=[x for w in allmembersW2 for x in allmembersW2[w]]        
        rnl2=self._getRealNodeLocations(allnodesforw)
        
        #t1=time.time()
        for r in R:
            nodem,waym,relm=[],[],[]
            for (xtype,xrole,x) in allmembersR.get(r,[]):
                if xtype==0:
                    nodem.append(x)
                if xtype==1:
                    waym.append(x)
                if xtype==2:
                    relm.append(x)    
                    
            #Only use nodes and ways?
            #rnl=self._getRealNodeLocations(nodem)
            lons=[rnl[x][0] for x in nodem if x in rnl]
            lats=[rnl[x][1] for x in nodem if x in rnl]
            
            #TX=self._getWayMembers(W)
            #allnodes=[x for w in TX for x in TX[w]]        
                        
            lons=lons+[rnl2[x][0] for w in waym if w in allmembersW2 for x in allmembersW2[w] if x in rnl2]
            lats=lats+[rnl2[x][1] for w in waym if w in allmembersW2 for x in allmembersW2[w] if x in rnl2]
            
            if len(lons)>0:
                res["R"+str(r)]=[np.mean(lons),np.mean(lats),min(lons),min(lats),max(lons),max(lats)]
            else:
                res["R"+str(r)]=[float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
        
        #t2=time.time()
        #print("diff",t2-t1)
        
        #print(allmembersR)
        
        #for wid in W:
        
        for _id in ids:
            if _id not in res:
                res[_id]=[float('nan'),float('nan'),float('nan'),float('nan'),float('nan'),float('nan')]
                
        return res
    
    
    def getKVs(self,k,vs,mtype):
        res=[]
        
        for v in vs:
            if mtype==0:            
                self.cur.execute("SELECT id,k,v FROM nodetags WHERE k=='"+k+"' and v=='"+v+"';")        
                for row in self.cur.fetchall():
                    res.append("N"+str(row[0]))
                    
            if mtype==1:            
                self.cur.execute("SELECT id,k,v FROM waytags WHERE k=='"+k+"' and v=='"+v+"';")        
                for row in self.cur.fetchall():
                    res.append("W"+str(row[0]))
            
            if mtype==2:            
                self.cur.execute("SELECT id,k,v FROM relationtags WHERE k=='"+k+"' and v=='"+v+"';")        
                for row in self.cur.fetchall():
                    res.append("R"+str(row[0]))
                
        return list(set(res))



    def getValues(self,ids,k):
        N,W,R=self._getRealIDs(ids)
        
        res={}
                            
        self.cur.execute("SELECT id,v FROM nodetags WHERE id in ("+",".join([str(id) for id in N])+") and k=='"+k+"';")        
        for row in self.cur.fetchall():
            res["N"+str(row[0])]=row[1]
            
    
        self.cur.execute("SELECT id,v FROM waytags WHERE id in ("+",".join([str(id) for id in W])+") and k=='"+k+"';")        
        for row in self.cur.fetchall():
            res["W"+str(row[0])]=row[1]
    
    
        self.cur.execute("SELECT id,v FROM relationtags tags WHERE id in ("+",".join([str(id) for id in R])+") and k=='"+k+"';")        
        for row in self.cur.fetchall():
            res["R"+str(row[0])]=row[1]
                
        return res

    def filterKVs(self,ids,filterlist):
        result=set(ids)
        
        N,W,R=self._getRealIDs(ids)
        for (k,vs) in filterlist:
            #print(k,vs)
            
            self.cur.execute("SELECT id,v FROM nodetags WHERE id in ("+",".join([str(id) for id in N])+") and k=='"+k+"';")        
            for row in self.cur.fetchall():     
                
                if row[1] in vs and "N"+str(row[0]) in result:
                    result.remove("N"+str(row[0]))
            
            self.cur.execute("SELECT id,v FROM waytags WHERE id in ("+",".join([str(id) for id in W])+") and k=='"+k+"';")        
            for row in self.cur.fetchall():                
                if row[1] in vs and "W"+str(row[0]) in result:
                    result.remove("W"+str(row[0]))
                
            self.cur.execute("SELECT id,v FROM relationtags WHERE id in ("+",".join([str(id) for id in R])+") and k=='"+k+"';")        
            for row in self.cur.fetchall():                
                if row[1] in vs and "R"+str(row[0]) in result:
                    result.remove("R"+str(row[0]))
                        
        
        return result


    def filterKVMembers(self,ids,filterlist):
        result=set(ids)
        
        N,W,R=self._getRealIDs(ids)
        for (k,vs) in filterlist:
            
            self.cur.execute("SELECT waytags.v,waymembers.child FROM waytags INNER JOIN waymembers ON waytags.id=waymembers.id where waymembers.child in ("+",".join([str(id) for id in N])+") AND k=='"+k+"';")
            for row in self.cur.fetchall():                        
                if row[0] in vs and "N"+str(row[1]) in result:
                    result.remove("N"+str(row[1]))
                                            
        return result

    def keepKVMembers(self,ids,filterlist):
        result=set()
        
        N,W,R=self._getRealIDs(ids)
        for (k,vs) in filterlist:
            
            self.cur.execute("SELECT waytags.v,waymembers.child FROM waytags INNER JOIN waymembers ON waytags.id=waymembers.id where waymembers.child in ("+",".join([str(id) for id in N])+") AND k=='"+k+"';")
            for row in self.cur.fetchall():                        
                if row[0] in vs:
                    result.add("N"+str(row[1]))                    
                                            
        return result



    def getAssignedStops(self,L):
        result=set()
        
        relations=set()
        for (k,v) in L:
            self.cur.execute("SELECT id FROM relationtags WHERE k=='"+k+"' AND v=='"+v+"';")        
            for row in self.cur.fetchall():                
                relations.add(row[0])
                
        T=self._getRelationMembers(relations)
        for tid in T:            
            for (xtype,xrole,x) in T[tid]:
                if (xrole=="stop" or xrole=="platform") and xtype==0:
                    result.add("N"+str(x))
                                                
        return result


    #def getTags(self,_id):
        #if _id[0]=="N":
            #return self.N[_id][-1]
        #elif _id[0]=="W":            
            #return self.W[_id][-1]
        #elif _id[0]=="R":            
            #return self.R[_id][-1]
