The project OSMRailway extracts the railway network from a given openstreetmap file in PBF format. The program has the following non-standard dependencies, which can be installed via pip:

osmium, umsgpack, chardet, sqlite3, string, pymp, editdistance, scipy.spatial, networkx, haversine, numpy


The program consists of two steps:

1) 1ExtractTransport.py: Reads a pbf file and extracts all transport-related items from it. 

2) 2CreateRailway.py: Create the railway network for a specific file

The osm file is defined in Settings.py. By default, it is REGION="berlin", which means that the program looks for a file berline-latest.osm.pbf in the root folder. OSM files can be obtained directly from Openstreetmap or geofabrik.de.

After exectuing 2CreateRailway.py, the root CSV will contain two new files: One file defines the nodes in the network (e.g. berlin_nodes.csv) and the other defines the links between nodes (e.g. berlin_links.csv).



If you use this program or data generated from it in a publication, please cite the following paper which published the methodology behind OSMRailway:

Wandelt, Sebastian, Zezhou Wang, and Xiaoqian Sun. 
"Worldwide railway skeleton network: Extraction methodology and preliminary analysis." 
IEEE Transactions on Intelligent Transportation Systems 18.8 (2017): 2206-2216.

Bibtex:

@article{wandelt2017worldwide,
  title={Worldwide railway skeleton network: Extraction methodology and preliminary analysis},
  author={Wandelt, Sebastian and Wang, Zezhou and Sun, Xiaoqian},
  journal={IEEE Transactions on Intelligent Transportation Systems},
  volume={18},
  number={8},
  pages={2206--2216},
  year={2017},
  publisher={IEEE}
}
