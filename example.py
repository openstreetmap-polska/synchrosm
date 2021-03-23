import synchrosm

if __name__ == '__main__':
    # download data and store in sqlite database
    query = '''
    [out:json][timeout:250][bbox:49,14.116,54.83,24.15];
    (
      node["amenity"="vending_machine"]["operator"~"inpost.*", i];
      node["amenity"="vending_machine"]["brand"~"inpost.*", i];
      node["amenity"="vending_machine"]["name"~".*inpost.*", i];
    );
    out meta qt;
    '''
    synchrosm.main.download_and_store_data(query)

    # check if new versions of nodes are in osm
    results = synchrosm.main.compare_db_data_to_api()
    print(results.new_version_in_osm)

    # match objects to osm
    objects = [
        {'id': 'KRA90M', 'latitude': 50.03852, 'longitude': 19.96645},
        {'id': 'KRA04N', 'latitude': 50.0387, 'longitude': 19.96578},
    ]

    db = synchrosm.database.DB()
    nodes = db.select_nodes()
    matches = synchrosm.main.match_objects(objects, nodes)
    print(matches)
