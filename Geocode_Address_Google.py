class pygeomaps:
    def __init__(self):
        import ConfigParser
        self.lat=0.0
        self.lon=0.0
        self.radius=0
        self.final_loc=[]
        self.addr=''
        self.placeid=''
        self.circle_colour='#ffcc99'
        config = ConfigParser.ConfigParser()
        config.read("GeoPy.conf")
        self.api_key=config.get('geopy_config', 'api_key')


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
            final_data_list = pgm.get_qualifying_locations(lat1,lon1,self.radius)
            return final_data_list
        except Exception, e:
            print(str(e))

    #Modify GMPLOT PACKAGE to include address as title for each marker
    def plot_addresses_on_google_map(self,lat,lon,titles):
        import gmplot
        # unit of radius in gmplot is meter. so it assumes KM passed as meter.So multiply by 1000
        gmap = gmplot.GoogleMapPlotter(self.lat,self.lon,16)
        gmap.coloricon = "http://www.googlemapsmarkers.com/v1/%s/"
        #gmap.plot(lat, lon, 'cornflowerblue', edge_width=10)
        gmap.scatter(lat, lon,titles,'#0066ff', edge_width=10)
        #gmap.heatmap(lat, lon,threshold=90,radius=10)
        print "Center Location for this circle : " + self.addr
        gmap.marker(self.lat,self.lon,color='#FF0000',title=self.addr)
        gmap.circle(self.lat,self.lon,self.radius*1000,color=self.circle_colour)
        output = "C:/GeoPy/mymap.html"
        gmap.draw(output)

    def removeNonAscii(self,s):
        return "".join(filter(lambda x: ord(x) < 128, s))

    def geocode_address(self,addr):
        import urllib2,urllib
        import untangle

        #Prepare the URL link
        address=urllib.quote_plus(addr)
        wiki = "http://maps.googleapis.com/maps/api/geocode/xml?address=" + address + "key=" + self.api_key
        geocode_result=[]

        #Query the website and return the data and parse the xml using untangle
        page = urllib2.urlopen(wiki)
        xmldata = page.read()
        obj = untangle.parse(xmldata)

        # If scrubbing status is OK then result should be received. Not sure how many result.
        scrb_sts = str(obj.GeocodeResponse.status.cdata).strip()
        if scrb_sts == "OK" :
            if(len(obj.GeocodeResponse.get_elements(name='result'))) == 1:
                scrb_addr = self.removeNonAscii(obj.GeocodeResponse.result.formatted_address.cdata)
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

    def get_qualifying_locations(self,center_lat,center_lon,distancekm):
        from mysql.connector import MySQLConnection, Error
        import math
        ret_list = []
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
            cursor = conn.cursor()
            query = "SELECT c.COMPANY_NAME, c.TIV,t.* FROM (SELECT RAW_ADDR,LATITUDE,LONGITUDE," \
                    "(acos(sin(radians(%s)) * sin(radians(Latitude)) + cos(radians(%s)) * cos(radians((Latitude))) " \
                    "* cos(radians(%s) - radians(Longitude))) * 6378.137) AS distance " \
                    "FROM  train_set.geo_scrub_addr where SCRUB_TYPE IN (%s,%s,%s) AND SCRUB_STS=%s) t," \
                    "(select COMPANY_NAME,COMPANY_ADDR,SUM(COMPANY_TIV) as TIV from " \
                    "train_set.company_info group by COMPANY_NAME,COMPANY_ADDR) c WHERE t.RAW_ADDR=c.COMPANY_ADDR  " \
                    "and t.distance <= %s"
            cursor.execute(query,[center_lat,center_lat,center_lon,'ROOFTOP','RANGE_INTERPOLATED','GEOMETRIC_CENTER','OK',distancekm])
            for (company_name,tiv,raw_addr,latitude,longitude,distance) in cursor:
                ret_list.append([company_name,tiv,raw_addr,latitude,longitude,distance])
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return ret_list

    def prepare_html_report(self,rpt_data,center_addr):
        try:
            HTMLFILE = 'C:/GeoPy/RiskCircle_Report.html'
            hdr_data=['Company Name','TIV','Address','Latitude','Longitude','Distance']
            f = open(HTMLFILE, 'w')
            f.write('<html><body><h1>')
            f.write('Risk Circle Report for Center :'+ center_addr)
            f.write('</h1>')
            f.write('<table border = "1">')
            f.write('<tr>')
            for head in hdr_data:
                f.write('<th>')
                f.write(head)
                f.write('</th>')
            f.write('</tr>')
            for data in rpt_data:
                f.write(data)
        except Exception, e1:
            print str(e1)


pgm = pygeomaps()
user_input=raw_input("What do you want to do?\n\t Press 1 for Geocode addresses from table"
                     "\n\t Press 2 for Creating a risk circle for any address based on a radius limit"
                         "\n\t Press 3 for geocoding a single random address"
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
    #[company_name,tiv,raw_addr,latitude,longitude,distance]
    lat=[]
    lon=[]
    title=[]
    rpt_data=[]
    total_tiv=0
    total_locs=len(addr_list)
    if total_locs > 0:
        for data in range(total_locs):
            lat.append(addr_list[data][3]) #Lat
            lon.append(addr_list[data][4]) #Lon
            title.append(str(addr_list[data][5])) #Distance in km from center
            rpt_data.append("<tr><td>" + str(addr_list[data][0]) +
                             "</td><td>" + str(addr_list[data][1]) +
                             "</td><td>" + str(addr_list[data][2]) +
                             "</td><td>" + str(addr_list[data][3]) +
                             "</td><td>" + str(addr_list[data][4]) +
                             "</td><td>" + str(addr_list[data][5]) +
                             "</tr>")
        pgm.plot_addresses_on_google_map(lat,lon,title)
        pgm.prepare_html_report(rpt_data,center_addr)
        print "Total Locations impacted : " + str(total_locs)
        print "Please check the generated report for details."
    else:
        print "No other existing locations impacted"
elif user_input == '3':
    result = pgm.geocode_address("5687 BRAMBLEWOOD RD., ALTADENA, CA 91001, CALIFORNIA, UNITED STATES")
    if len(result) !=0:
        for value in range(len(result)):
            print result[value]
else:
    print "You selected to exit.Have a nice day...."
    exit()
