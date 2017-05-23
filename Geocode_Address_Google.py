class pygeomaps:
    def __init__(self):
        self.lat=[]
        self.lon=[]
        self.geocode_result=[]

    def find_distance_between_lat_lon(self,lat1,lat2,lon1,lon2):
        import math
        d= math.acos(math.sin(math.radians(lat1)) * math.sin(math.radians(lat2)) + math.cos(math.radians(lat1)) *
                     math.cos(math.radians((lat2))) * math.cos(math.radians(lon1) - math.radians(lon2)))
        #To search by miles instead of kilometers, replace 6371 with 3959
        distance = 6371*d
        return distance

    def plot_addresses_on_google_map(self,lat,lon):
        import gmplot
        gmap = gmplot.GoogleMapPlotter(lat[0], lon[0],16)
        gmap.plot(self.lat, self.lon, '#FF0000', edge_width=5)
        gmap.heatmap(self.lat, self.lon,threshold=20,radius=20)
        output = "C:/GeoPy/mymap.html"
        gmap.draw(output)

    def geocode_address(self,addr):
        import urllib2,urllib
        import untangle

        #Prepare the URL link
        address=urllib.quote_plus(addr)
        wiki = "http://maps.googleapis.com/maps/api/geocode/xml?address=" + address
        geocode_result=[]
        print wiki

        #Query the website and return the data and parse the xml using untangle
        page = urllib2.urlopen(wiki)
        xmldata = page.read()
        obj = untangle.parse(xmldata)

        # If scrubbing status is OK then result should be received. Not sure how many result.
        scrb_sts = str(obj.GeocodeResponse.status.cdata).strip()
        if scrb_sts == "OK" :
            if(len(obj.GeocodeResponse.get_elements(name='result'))) == 1:
                scrb_addr = str(obj.GeocodeResponse.result.formatted_address.cdata)
                type_len = len(obj.GeocodeResponse.result.get_elements(name='type'))
                if (type_len) == 1:
                    addr_typ = str(obj.GeocodeResponse.result.type.cdata)
                else:
                    addr_typ = str(obj.GeocodeResponse.result.type[0].cdata)
                    for data in range(1,type_len):
                        addr_typ = addr_typ + ":" + str(obj.GeocodeResponse.result.type[data].cdata)
                place_id = str(obj.GeocodeResponse.result.place_id.cdata)
                for item in obj.GeocodeResponse.result.geometry:
                    lat = float(str(item.location.lat.cdata).strip())
                    lon = float(str(item.location.lng.cdata).strip())
                    loc_type = str(item.location_type.cdata).strip()
                    vp_lat_southwest = float(str(item.viewport.southwest.lat.cdata))
                    vp_lon_southwest = float(str(item.viewport.southwest.lng.cdata))
                    vp_lat_northeast = float(str(item.viewport.northeast.lat.cdata))
                    vp_lon_northeast = float(str(item.viewport.northeast.lng.cdata))
            else:
                # If multiple results returned, consider the 1st result
                scrb_addr = str(obj.GeocodeResponse.result[0].formatted_address.cdata)
                type_len = len(obj.GeocodeResponse.result[0].get_elements(name='type'))
                if (type_len) == 1:
                    addr_typ = str(obj.GeocodeResponse.result[0].type.cdata)
                else:
                    addr_typ = str(obj.GeocodeResponse.result[0].type[0].cdata)
                    for data in range(1, type_len):
                        addr_typ = addr_typ + ":" + str(obj.GeocodeResponse.result[0].type[data].cdata)
                place_id = str(obj.GeocodeResponse.result[0].place_id.cdata)
                for item in obj.GeocodeResponse.result[0].geometry:
                    lat = float(str(item.location.lat.cdata).strip())
                    lon = float(str(item.location.lng.cdata).strip())
                    loc_type = str(item.location_type.cdata).strip()
                    vp_lat_southwest = float(str(item.viewport.southwest.lat.cdata))
                    vp_lon_southwest = float(str(item.viewport.southwest.lng.cdata))
                    vp_lat_northeast = float(str(item.viewport.northeast.lat.cdata))
                    vp_lon_northeast = float(str(item.viewport.northeast.lng.cdata))

            geocode_result.append(addr)
            geocode_result.append(scrb_sts)
            geocode_result.append(addr_typ)
            geocode_result.append(loc_type)
            geocode_result.append(scrb_addr)
            geocode_result.append(wiki)
            geocode_result.append(lat)
            geocode_result.append(lon)
            geocode_result.append(vp_lat_southwest)
            geocode_result.append(vp_lon_southwest)
            geocode_result.append(vp_lat_northeast)
            geocode_result.append(vp_lon_northeast)
            geocode_result.append(place_id)
            return geocode_result
            #confirmation = raw_input("Plot thhe address on map?(y/n)")
            #if(confirmation=="y"):
                #self.plot_on_google_map(lat,lon,place_id)
        else:
            geocode_result.append(addr)
            geocode_result.append(scrb_sts)
            geocode_result.append('NOK')
            geocode_result.append('NOK')
            geocode_result.append('NOK')
            geocode_result.append(wiki)
            geocode_result.append(0.0)
            geocode_result.append(0.0)
            geocode_result.append(0.0)
            geocode_result.append(0.0)
            geocode_result.append(0.0)
            geocode_result.append(0.0)
            geocode_result.append('NOK')
            return geocode_result


    def read_addresses_to_process(self):
        from mysql.connector import MySQLConnection,Error
        try:
            addr_list =[]
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            cursor.execute("SELECT COMPANY_ADDR FROM train_set.company_info where IS_SCRUBBED IS NULL")
            #cursor returns a tuple
            for (row,) in cursor:
                #print row
                addr_list.append(row)
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return addr_list

    def insert_geocode_result(self,gr):
        from mysql.connector import MySQLConnection, Error

        query = "INSERT INTO " \
        "train_set.geo_scrub_addr(RAW_ADDR,SCRUB_STS,ADDR_TYPE,SCRUB_TYPE,SCRUB_ADDR,REQUEST_LINK,LATITUDE,LONGITUDE," \
        "SWLATITUDE,SWLONGITUDE,NELATITUDE,NELONGITUDE,G_PLACE_ID) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #args = (gr[0],gr[1],gr[2],gr[3],gr[4],gr[5],gr[6],gr[7],gr[8],gr[9],gr[10],gr[11],gr[12])
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            cursor.execute(query, gr)
            cursor.execute("UPDATE train_set.company_info set IS_SCRUBBED=%s where COMPANY_ADDR=%s",('D',gr[0]))
            # accept the changes
            conn.commit()
        except Error as error:
            print(error)
        finally:
            cursor.close()
            conn.close()


pgm = pygeomaps()
link_list = pgm.read_addresses_to_process()
if len(link_list) == 0:
    print "No Records to process in the table."
else:
    for link in link_list:
        gr=pgm.geocode_address(link)
        pgm.insert_geocode_result(gr)

#pgm.plot_addresses_on_google_map(pgm.lat,pgm.lon)
# d=pgm.find_distance_between_lat_lon(36.1137276,36.1378447,-115.1523206,-115.1654364)
# #2880 S Las Vegas Blvd, Las Vegas, NV 89109, USA
# #4100 Paradise Rd, Las Vegas, NV 89169, USA
# print 'Distance is :' + str(d) + " KMs"
