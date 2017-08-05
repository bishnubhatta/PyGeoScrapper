class PyScrapper:
    def __init__(self):
        print "This program will scrape company data from yellowpagesusa.net and load in to a table."
        print "Address for Geocoding and analysis will be taken from here."
        print "Initializing Pyscrapper......"


    def read_data_from_url(self,url,state):
        from lxml import html
        import requests
        from mysql.connector import MySQLConnection, Error
        try:

            page = requests.get(url)
            tree = html.fromstring(page.content)

            company_info_list = []
            # Company Name is mandatory
            company_name = tree.xpath('//div/h3[@class="title"]/a/text()')

            # Default missing phone numbers
            company_phone = []
            path1 = '//div[@class="main"]/div[@class="content page"]/div/p[1]'
            path2 = './span[@class="tel"]'

            # Compare if path2 exists in path1+path2
            for link in tree.xpath(path1):
                other_node = link.xpath(path2)
                if len(other_node):
                    company_phone.append(other_node[0].text)
                else:
                    company_phone.append("NOT_AVAILABLE")

            # Default missing phone numbers
            company_fax = []
            path1 = '//div[@class="main"]/div[@class="content page"]/div/p[1]'
            path2 = './span[@class="fax"]'

            for link in tree.xpath(path1):
                other_node = link.xpath(path2)
                if len(other_node):
                    company_fax.append(other_node[0].text)
                else:
                    company_fax.append("NOT_AVAILABLE")

            # Company Addr is mandatory
            company_addr = tree.xpath('//div/p[2]/text()')

            for i in range(0, len(company_name)):
                company_data = []
                company_data.append(str(company_name[i]).upper())
                company_data.append(str(company_phone[i]))
                company_data.append(str(company_fax[i]))
                company_data.append(str(company_addr[i]).upper())
                company_info_list.append(company_data)

            # Insert the scrapped link to scrap_link table
            query = "INSERT INTO train_set.scrap_link(LINK,STATE,SCRAP_TM) " \
                    "values (%s,%s,NOW())"
            info = [url,state]
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
            cursor = conn.cursor()
            cursor.execute(query, info)
            conn.commit()
            cursor.close()
            conn.close()
            return [company_info_list]
        except Exception, e1:
            print str(e1)


    def prepare_url_link(self,baseurl):
        import string
        urllist=[]
        #Remove the .html from the link
        baseurl = string.replace(string.replace(baseurl,'.html',''),'City','search')
        for alpha in ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']:
            urllist.append('http://yellowpagesusa.net/'+baseurl+'-with-'+alpha+'.html')
        return urllist

    def get_baseurl_from_db(self,state,limit):
        from mysql.connector import MySQLConnection,Error
        base_uri = []
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
            cursor = conn.cursor()
            qry = "SELECT ADDR_BASE_URL FROM train_set.scrap_addr_link where STATE =%s and PROCESS_FLAG = %s limit %s"
            args=[state,'N',limit]
            cursor.execute(qry, args)
            #cursor returns a tuple
            for (row,) in cursor:
                base_uri.append(row)
            for uri in range(len(base_uri)):
                qry = "UPDATE train_set.scrap_addr_link set PROCESS_FLAG=%s where PROCESS_FLAG=%s and STATE=%s and ADDR_BASE_URL=%s"
                args = ['D', 'N', state, base_uri[uri]]
                cursor.execute(qry, args)
                conn.commit()
            return base_uri
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return base_uri

    def insert_data_into_company_table(self,info):
        from mysql.connector import MySQLConnection,Error
        import random
        base_uri = []
        query = "INSERT INTO train_set.company_info(COMPANY_NAME,COMPANY_PHONE,COMPANY_FAX,COMPANY_ADDR,COMPANY_TIV) " \
                "values (%s,%s,%s,%s,%s)"
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
            cursor = conn.cursor()
            # As it is not possible to get TIV for each company, assign a random amount
            info.append(random.randint(25000,99999))
            cursor.execute(query,info)
            conn.commit()
        except Error as error:
            print(error)
        finally:
            cursor.close()
            conn.close()

    # For restarting, this will give you links those are already scrubbed
    def fetch_already_scrap_link(self,state):
        from mysql.connector import MySQLConnection, Error
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
            cursor = conn.cursor()
            url_list_st=[]
            qry = "SELECT LINK FROM train_set.scrap_link where STATE=%s"
            args = [state]
            cursor.execute(qry, args)
            # cursor returns a tuple
            for (row,) in cursor:
                url_list_st.append(row)
            return url_list_st
        except Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
            return url_list_st

class pygeomaps:
    def __init__(self):
        import ConfigParser
        config = ConfigParser.ConfigParser()
        config.read('GeoPy.conf')
        self.api_key=config.get('geopy_config', 'api_key')
        self.place_id_search_link=config.get('geopy_config', 'place_id_search_link')
        self.nearby_search_link_location=config.get('geopy_config', 'nearby_search_link_location')
        self.nearby_text_search_link=config.get('geopy_config', 'nearby_text_search_link')
        self.nearby_radar_search_link=config.get('geopy_config', 'nearby_radar_search_link')
        self.street_view_image_link=config.get('geopy_config', 'street_view_image_link')
        self.static_map_link = config.get('geopy_config', 'static_map_link')
        self.geocode_link=config.get('geopy_config', 'geocode_link')
        self.lat=0.0
        self.lon=0.0
        self.radius=0
        self.final_loc=[]
        self.addr=''
        self.place_type=''
        self.rpt_name=''
        self.circle_colour='#ffcc99'

    def cleanup_dir(self,mydir):
        try:
            import os
            for file in os.listdir(mydir):
                filepath= os.path.join(mydir, file)
                if os.path.isfile(filepath):
                    os.unlink(filepath)
        except Exception as e:
            print(e)

    def get_street_view_image(self,addr,heading=0):
        import urllib,os
        # Prepare the URL link
        address = urllib.quote_plus(addr)
        # optional
        imageurl = self.street_view_image_link + "?size=1200x800&location="+address+"&heading=" +str(heading)+"&pitch=-0.76&key="+ self.api_key
        # change space to underscore and remove , from the address if any
        final_file=os.path.join(r"/home/bishnu/GeoPy/images/", addr.replace(' ','_').replace(',','').replace('#','Z')+"_"+str(heading)+".jpg")
        urllib.urlretrieve(imageurl, final_file)
        return final_file.replace('/home/bishnu/GeoPy','')

    def verify_if_street_view_image_exists(self,addr):
        import urllib,urllib2,json
        address = urllib.quote_plus(addr)
        metaurl="https://maps.googleapis.com/maps/api/streetview/metadata?size=1200x800&location="+ address+"&key="+ self.api_key
        page = urllib2.urlopen(metaurl)
        config = json.loads(page.read())
        return 'OK' if config['status']=='OK' else 'NOK'

    def get_tiv_for_addr(self,addr):
        from mysql.connector import MySQLConnection,Error
        tiv_amt = 0
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
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

    def find_all_addresses_with_in_x_km_radius_of_y(self,y,x,option):
        try:
            import math
            center_addr=y
            self.radius=x
            gcode_input_addr=self.geocode_address(center_addr)
            self.lat = gcode_input_addr[6] #Setting center's latitude
            self.lon= gcode_input_addr[7] #Setting center's longitude
            self.addr= gcode_input_addr[4] #Setting center's address
            final_data_list = []
            if option == '2': # For risk circle, data will be fetched from table
                final_data_list = pgm.get_qualifying_locations_risk_circle(self.lat,self.lon,self.radius)
            else:
                final_data_list = pgm.find_places_around_center(self.radius,self.place_type)
            return final_data_list
        except Exception, e:
            print(str(e))

    def plot_addresses_on_google_map(self,lat,lon,titles):
        import gmplot
        # unit of radius in gmplot is meter. so it assumes KM passed as meter.So multiply by 1000
        gmap = gmplot.GoogleMapPlotter(self.lat,self.lon,16)
        gmap.coloricon = "http://www.googlemapsmarkers.com/v1/%s/"
        gmap.scatter(lat, lon,titles,'#00FF00', edge_width=10)
        print "Center Location for this circle : " + self.addr
        gmap.marker(self.lat,self.lon,color='#FF0000',title=self.addr)
        #addListenersOnMarker(title, "http://www.google.com");
        gmap.circle(self.lat,self.lon,self.radius*1000,color=self.circle_colour)
        output = "/home/bishnu/GeoPy/"+self.rpt_name+"_gmap.html"
        gmap.draw(output)

    def generate_static_map_for_location(self,addr,maptype):
        import urllib, os
        # Prepare the URL link
        address = urllib.quote_plus(addr)
        # optional
        imageurl = self.static_map_link + "?maptype="+maptype+"&center="+address+"&zoom=18&size=1200x800&key=" + self.api_key
        # change space to underscore and remove , from the address if any
        image = os.path.join(r"/home/bishnu/GeoPy/images/",addr.replace(' ', '_').replace(',', '') + "_" +maptype +"_image.jpg")
        urllib.urlretrieve(imageurl, image)

    def generate_map_for_location(self,address):
        import gmplot
        self.addr=address
        self.rpt_name='map_for_location_gmap'
        det = self.geocode_address(self.addr)
        self.lat=det[6]
        self.lon=det[7]
        gmap = gmplot.GoogleMapPlotter(self.lat, self.lon, 18)
        gmap.coloricon = "http://www.googlemapsmarkers.com/v1/%s/"
        gmap.marker(self.lat, self.lon, color='#FF0000', title=self.addr)
        output = "/home/bishnu/GeoPy/"+self.rpt_name+".html"
        gmap.draw(output)

    def removeNonAscii(self,s):
        return "".join(filter(lambda x: ord(x) < 128, s))

    def geocode_address(self,addr):
        import urllib2,urllib
        import untangle
        #Prepare the URL link
        address=urllib.quote_plus(addr)
        wiki = self.geocode_link + "?address=" + address
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
            geocode_result.append(addr)
            geocode_result.append(scrb_sts)
            geocode_result.append(addr_typ)
            geocode_result.append(loc_type)
            geocode_result.append(scrb_addr)
            geocode_result.append(wiki)
            geocode_result.append(lat)
            geocode_result.append(lon)
            geocode_result.append(place_id)
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
            geocode_result.append('NOK')
            return geocode_result


    def read_addresses_to_process(self):
        from mysql.connector import MySQLConnection,Error
        try:
            addr_list =[]
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
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

        query = "INSERT INTO train_set.geo_scrub_addr" \
                "(RAW_ADDR,SCRUB_STS,ADDR_TYPE,SCRUB_TYPE,SCRUB_ADDR,REQUEST_LINK,LATITUDE,LONGITUDE,G_PLACE_ID) " \
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
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

    def get_qualifying_locations_risk_circle(self,center_lat,center_lon,distancekm):
        from mysql.connector import MySQLConnection, Error
        import math
        ret_list = []
        try:
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='bishnu')
            cursor = conn.cursor()
            query = "SELECT c.COMPANY_NAME, c.TIV,t.* FROM (SELECT RAW_ADDR,LATITUDE,LONGITUDE," \
                    "(acos(sin(radians(%s)) * sin(radians(Latitude)) + cos(radians(%s)) * cos(radians((Latitude))) " \
                    "* cos(radians(%s) - radians(Longitude))) * 6378.8) AS distance " \
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

    def calculate_distance_between_2_lat_lon(self,Latitude,Longitude):
        import math
        distance = math.acos(math.sin(math.radians(self.lat)) * math.sin(math.radians(Latitude)) +
                   math.cos(math.radians(self.lat)) * math.cos(math.radians((Latitude))) *
                             math.cos(math.radians(self.lon) - math.radians(Longitude))) * 6378.8
        return distance

    def find_places_around_center(self,distance_in_km,place_type):
        import urllib2, urllib
        import untangle
        # Prepare the URL link
        wiki = self.nearby_search_link_location + "?location=" + str(self.lat)+","+str(self.lon)+\
               "&radius=" + str(distance_in_km*1000) + "&type=" + self.place_type + "&key=" + self.api_key
        final_list = []
        # Query the website and return the data and parse the xml using untangle
        page = urllib2.urlopen(wiki)
        xmldata = page.read()
        obj = untangle.parse(xmldata)
        # If scrubbing status is OK then result should be received. Not sure how many result.
        srch_sts = str(obj.PlaceSearchResponse.status.cdata).strip()
        if srch_sts == "OK":
            result_length = len(obj.PlaceSearchResponse.get_elements(name='result'))
            if result_length == 1:
                company_name =  str(self.removeNonAscii(obj.PlaceSearchResponse.result.name.cdata))
                latitude =  float(str(obj.PlaceSearchResponse.result.geometry.location.lat.cdata))
                longitude =  float(str(obj.PlaceSearchResponse.result.geometry.location.lng.cdata))
                #print str(obj.PlaceSearchResponse.result.place_id.cdata)
                distance = self.calculate_distance_between_2_lat_lon(latitude,longitude)
                final_list.append([company_name, latitude, longitude, distance])
            else:
                for i in range(0,result_length):
                    company_name =  str(self.removeNonAscii(obj.PlaceSearchResponse.result[i].name.cdata))
                    latitude =  float(str(obj.PlaceSearchResponse.result[i].geometry.location.lat.cdata))
                    longitude =  float(str(obj.PlaceSearchResponse.result[i].geometry.location.lng.cdata))
                    #print str(obj.PlaceSearchResponse.result[i].place_id.cdata)
                    distance = self.calculate_distance_between_2_lat_lon(latitude,longitude)
                    final_list.append([company_name, latitude, longitude, distance])
        return final_list

    def prepare_html_report(self,hdr_data,rpt_data):
        try:
            HTMLFILE = '/home/bishnu/GeoPy/'+ self.rpt_name + '_rpt.html'
            f = open(HTMLFILE, 'w')
            f.write('<html><body><h1>')
            f.write('Report for Center :'+ self.addr)
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
                        "\n\t Press 4 for finding places of interest around a specific address"
                        "\n\t Press 5 for Plotting an address on map"
                        "\n\t Press 6 for scraping website company address data into table"
                        "\n\t Press 7 for image for an address"
                        "\n\t Press 8 for static map image for an address"
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
    addr_list = pgm.find_all_addresses_with_in_x_km_radius_of_y(center_addr,rad,user_input)
    #[company_name,tiv,raw_addr,latitude,longitude,distance]
    lat=[]
    lon=[]
    title=[]
    rpt_data=[]
    total_tiv=0
    total_locs=len(addr_list)
    pgm.cleanup_dir(r"/home/bishnu/GeoPy/images/")
    if total_locs > 0:
        for data in range(total_locs):
            lat.append(addr_list[data][3]) #Lat
            lon.append(addr_list[data][4]) #Lon
            title.append(str(addr_list[data][5])) #Distance in km from center
            status = pgm.verify_if_street_view_image_exists(str(addr_list[data][2]))
            if status == 'OK':
                img_loc = '.'+pgm.get_street_view_image(str(addr_list[data][2]))
            else:
                img_loc = 'https://st.depositphotos.com/1779253/5140/v/950/depositphotos_51407019-stock-illustration-404-error-file-not-found.jpg'
            rpt_data.append("<tr><td>" + str(addr_list[data][0]) +
                             "</td><td>" + str(addr_list[data][1]) +
                             "</td><td>" + str(addr_list[data][2]) +
                             "</td><td>" + str(addr_list[data][3]) +
                             "</td><td>" + str(addr_list[data][4]) +
                             "</td><td>" + str(addr_list[data][5]) +
                             "</td><td> <a href = " + img_loc +"> Image_Link </a>" +
                             "</tr>")
        pgm.rpt_name='risk_circle'
        pgm.plot_addresses_on_google_map(lat,lon,title)
        hdr_data = ['Company Name', 'TIV', 'Address', 'Latitude', 'Longitude', 'Distance', 'Image']
        pgm.prepare_html_report(hdr_data,rpt_data)
        print "Total Locations impacted : " + str(total_locs)
        print "Please check the generated report for details."
    else:
        print "No other existing locations impacted"
elif user_input == '3':
    center_addr = raw_input("\nPlease enter the location to geocode:\n")
    result = pgm.geocode_address(center_addr)
    if len(result) !=0:
        print 'Address sent for Geocoding:  ' + result[0]
        print 'Address returned from Geocoding: ' + result[4]
        print 'Confidence Level:  ' + result[3]
        print 'Latitude: ' + str(result[6])
        print 'Longitude: ' + str(result[7])
        print 'Get me more details: ' + result[5]
elif user_input == '4':
    center_addr = raw_input("\nPlease enter the center address:\n")
    rad = int(raw_input("\nPlease enter the radius limit in KM:\n"))
    pgm.place_type = raw_input("\nPlease enter the place type:\n")
    result = pgm.geocode_address(center_addr)
    # Set the global variable to use as center during map plotting
    pgm.lat = round(result[6], 6)
    pgm.lon = round(result[7], 6)
    pgm.addr = result[4]
    if len(result) !=0:
        print str(pgm.lat) + "," + str(pgm.lon)
        addr_list = pgm.find_all_addresses_with_in_x_km_radius_of_y(center_addr,rad,user_input)
        #[place, latitude, longitude, distance]
        lat = []
        lon = []
        title = []
        rpt_data = []
        total_locs = len(addr_list)
        if total_locs > 0:
            for data in range(total_locs):
                lat.append(addr_list[data][1])  # Lat
                lon.append(addr_list[data][2])  # Lon
                title.append(str(addr_list[data][0]))  # place name
                rpt_data.append("<tr><td>" + str(addr_list[data][0]) +
                                "</td><td>" + str(addr_list[data][1]) +
                                "</td><td>" + str(addr_list[data][2]) +
                                "</td><td>" + str(addr_list[data][3]) +
                                "</tr>")
            pgm.rpt_name = 'place_interest_' + pgm.place_type
            pgm.plot_addresses_on_google_map(lat, lon, title)
            hdr_data = ['Name', 'Latitude', 'Longitude', 'Distance']
            pgm.prepare_html_report(hdr_data, rpt_data)
elif user_input == '5':
    center_addr = raw_input("\nPlease enter the address to plot on map:\n")
    pgm.generate_map_for_location(center_addr)
elif user_input == '6':
    psc = PyScrapper()
    state = raw_input("\nPlease Enter the US state code to process:\n")
    limit = int(raw_input("\nPlease Enter the limit:(Ideal value 1-15)\n"))
    urllist = psc.get_baseurl_from_db(state, limit)
    for url in range(len(urllist)):
        final_link = psc.prepare_url_link(urllist[url])
        scrapped_link = psc.fetch_already_scrap_link(state)
        for link in range(len(final_link)):
            if final_link[link] not in scrapped_link:
                company_info_list = psc.read_data_from_url(final_link[link], state)
                for company_info in range(len(company_info_list)):
                    for indiv_company in range(len(company_info_list[company_info])):
                        psc.insert_data_into_company_table(company_info_list[company_info][indiv_company])
            else:
                print 'Link already scrapped!!!'
elif user_input == '7':
    addr = raw_input("\nPlease enter the address to fetch the image:\n")
    status = pgm.verify_if_street_view_image_exists(addr)
    if status == 'OK':
        print 'Congratulations!!! Image is available.\n'
        #heading = raw_input("\nPlease enter the heading for the image(0-360):\n")
        for heading in range(0,360,30):
            pgm.get_street_view_image(addr,heading)
    else:
        print 'No image exists for the location'
elif user_input == '8':
    address = raw_input("\nPlease enter the address to generate static map:\n")
    maptype = raw_input("\nPlease enter the map type (roadmap, satellite, hybrid, terrain):\n")
    pgm.generate_static_map_for_location(address,maptype)
else:
    print "You selected to exit.Have a nice day...."
    exit()
