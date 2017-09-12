import osmium as o

import sys
import os
import os.path

import Settings

keepd={}
keepd["r"]=set()
keepd["w"]=set()
keepd["n"]=set()


def needToKeep(X):
    keep=False
    if ("bus" in X.tags or "tram" in X.tags or "rail" in X.tags or "subway" in X.tags or "railway" in X.tags or "public_transport" in X.tags or "station" in X.tags or "aerialway" in X.tags):
        keep=True
    
    if "site" in X.tags: 
        if X.tags["site"] in ["stop_area","stop_area_group"]:
            keep=True

    if "building" in X.tags: 
        if X.tags["building"] in ["train_station","station"]:
            keep=True

    if "route" in X.tags: 
        if X.tags["route"] in ["bus","tram","train","railway","light_rail","rail","subway","ferry"]:
            keep=True
    
    if "highway" in X.tags: 
        if X.tags["highway"] in ["bus_stop","tram_stop","forward_stop","backward_stop"]:
            keep=True
            
    if "ferry" in X.tags: 
        if X.tags["ferry"] in ["yes"]:
            keep=True    
            
    if "amenity" in X.tags: 
        if X.tags["amenity"] in ["ferry_terminal","bus_station"]:
            keep=True        
    
    
    return keep

class RelationFilter(o.SimpleHandler):
    def __init__(self,):
        o.SimpleHandler.__init__(self)    
    
    def relation(self, r):        
        keep=needToKeep(r)        
        if keep or r.id in keepd["r"]:        
            keepd["r"].add(r.id)
            for m in r.members:
                keepd[m.type].add(m.ref)

class WayFilter(o.SimpleHandler):
    def __init__(self,):
        o.SimpleHandler.__init__(self)    

    def way(self, w):
        keep=needToKeep(w)                
        
        if keep or w.id in keepd["w"]:        
            keepd["w"].add(w.id)
            for m in w.nodes:
                keepd["n"].add(m.ref)

class NodeFilter(o.SimpleHandler):
    def __init__(self,):
        o.SimpleHandler.__init__(self)    

    def node(self, n):        
        keep=needToKeep(n)                                    
        
        if keep or n.id in keepd["n"]:        
            keepd["n"].add(n.id)

class Convert(o.SimpleHandler):

    def __init__(self, writer):
        o.SimpleHandler.__init__(self)
        self.writer = writer

    def node(self, n):
        if n.id in keepd["n"]:
            self.writer.add_node(n)

    def way(self, w):
        if w.id in keepd["w"]:
            self.writer.add_way(w)

    def relation(self, r):
        if r.id in keepd["r"]:
            self.writer.add_relation(r)



sfrom=Settings.REGION+"-latest.osm.pbf"
sto=Settings.REGION+"-latest-transport.osm.pbf"

if not os.path.isfile(sto) or Settings.CREATE_FROM_SCRATCH:
    tf = RelationFilter()
    tf.apply_file(sfrom)    
    print(len(keepd["r"]),len(keepd["w"]),len(keepd["n"]))

    tf = WayFilter()
    tf.apply_file(sfrom)    
    print(len(keepd["r"]),len(keepd["w"]),len(keepd["n"]))

    tf = NodeFilter()
    tf.apply_file(sfrom)    
    print(len(keepd["r"]),len(keepd["w"]),len(keepd["n"]))


    try:
        os.remove(sto)
    except OSError:
        pass

    writer = o.SimpleWriter(sto)
    handler = Convert(writer)
    handler.apply_file(sfrom)
    writer.close()
