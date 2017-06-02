class pygeomaps:
    def __init__(self):
        self.lat=0.0
        self.lon=0.0
        self.radius=0
        self.final_loc=[]
        self.addr=''
        self.placeid=''
        self.circle_colour='#ffcc99'

    def find_distance_between_two_location(self,lat1,lon1,lat2,lon2):
        import math
        d = math.acos(math.sin(math.radians(lat1)) * math.sin(math.radians(lat2)) + math.cos(math.radians(lat1)) *
                      math.cos(math.radians((lat2))) * math.cos(math.radians(lon1) - math.radians(lon2)))
        # To search by miles instead of kilometers, replace 6378.8 with 3963.6
        distance = 6378.8 * float(d)
        return distance

    def get_all_lat_lon_from_table(self):
        from mysql.connector import MySQLConnection,Error
        ret_list = []
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            cursor.execute("SELECT SCRUB_ADDR,LATITUDE,LONGITUDE,RAW_ADDR FROM train_set.geo_scrub_addr where SCRUB_STS=%s "
                           "and SCRUB_TYPE in (%s,%s,%s)",['OK','ROOFTOP','RANGE_INTERPOLATED','GEOMETRIC_CENTER'])
            for (scrub_addr,latitude,longitude,raw_addr) in cursor:
                ret_list.append([scrub_addr,latitude,longitude,raw_addr])
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return ret_list

    def get_tiv_for_addr(self,addr):
        from mysql.connector import MySQLConnection,Error
        tiv_amt = 0
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            cursor.execute("SELECT sum(COMPANY_TIV),COMPANY_ADDR FROM train_set.COMPANY_INFO "
                           "where IS_SCRUBBED=%s and COMPANY_ADDR=%s GROUP BY COMPANY_ADDR",['D',addr])
            (tiv_amt,adr)=cursor.fetchone()
            #print tiv_amt
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return tiv_amt

    def find_all_addresses_with_in_x_km_radius_of_y(self,y,x):
        try:
            import math
            input_addr=y
            self.radius=x
            gcode_input_addr=self.geocode_address(input_addr)
            lat1 = gcode_input_addr[6]
            lon1= gcode_input_addr[7]
            final_data_list = []
            #Get all lat lon from table
            ret_list=self.get_all_lat_lon_from_table()
            for list in range(len(ret_list)):
                addr=ret_list[list][0]
                lat2=float(ret_list[list][1])
                lon2=float(ret_list[list][2])
                distance = self.find_distance_between_two_location(lat1,lon1,lat2,lon2)
                if distance <= self.radius:
                    final_data_list.append(ret_list[list])
            self.final_loc = final_data_list
            return final_data_list
        except Exception, e:
            print(str(e))

    def plot_addresses_on_google_map(self,lat,lon):
        import gmplot
        # unit of radius in gmplot is meter. so it assumes KM passed as meter.So multiply by 1000
        gmap = gmplot.GoogleMapPlotter(self.lat,self.lon,16)
        gmap.coloricon = "http://www.googlemapsmarkers.com/v1/%s/"
        #gmap.plot(lat, lon, 'cornflowerblue', edge_width=10)
        gmap.scatter(lat, lon, '#0066ff', edge_width=10)
        #gmap.heatmap(lat, lon,threshold=90,radius=10)
        print "Center Location for this circle : " + self.addr
        gmap.marker(self.lat,self.lon,color='#FF0000',title=self.addr)
        gmap.circle(self.lat,self.lon,self.radius*1000,color=self.circle_colour)
        output = "C:/GeoPy/mymap.html"
        gmap.draw(output)

    def geocode_address(self,addr):
        import urllib2,urllib
        import untangle

        #Prepare the URL link
        address=urllib.quote_plus(addr)
        wiki = "http://maps.googleapis.com/maps/api/geocode/xml?address=" + address
        geocode_result=[]

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
            # Set the global variable to use as center during map plotting
            self.lat=round(lat,6)
            self.lon=round(lon,6)
            self.addr = scrb_addr
            self.placeid = place_id
            return geocode_result
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
            self.lat = 0.0
            self.lon = 0.0
            return geocode_result


    def read_addresses_to_process(self):
        from mysql.connector import MySQLConnection,Error
        try:
            addr_list =[]
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            cursor.execute("SELECT distinct(COMPANY_ADDR) FROM train_set.company_info where IS_SCRUBBED IS NULL")
            #cursor returns a tuple
            for (row) in cursor:
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
user_input=raw_input("What do you want to do?\n\t Press 1 for Geocode addresses from table"
                     "\n\t Press 2 for Creating a risk circle for any address based on a radius limit"
                         "\n\t Press any other key to exit\n\t:")
if user_input=='1':
    link_list = pgm.read_addresses_to_process()
    if len(link_list) == 0:
        print "No Records to process in the table."
    else:
        for (link,) in link_list:
            print link
            gr=pgm.geocode_address(link)
            pgm.insert_geocode_result(gr)
elif user_input == '2':
    #Make sure that this address is a valid ROOFTOP address eg: Walmart Store , Dell Office and not some random address
    # Radius is in KM
    center_addr=raw_input("\nPlease enter the center address:\n")
    rad = int(raw_input("\nPlease enter the radius limit in KM:\n"))
    addr_list = pgm.find_all_addresses_with_in_x_km_radius_of_y(center_addr,rad)
    lat=[]
    lon=[]
    total_tiv=0
    total_locs=len(addr_list)
    if total_locs > 0:
        for data in range(total_locs):
            lat.append(addr_list[data][1])
            lon.append(addr_list[data][2])
            total_tiv = total_tiv+ pgm.get_tiv_for_addr(addr_list[data][3])
        pgm.plot_addresses_on_google_map(lat,lon)
        print "Total Locations impacted : " + str(total_locs)
        print "Total TIV : " + str(total_tiv)
    else:
        print "Thank God!!! No other existing locations impacted."
else:
    print "You selected to exit.Have a nice day...."
    exit()
