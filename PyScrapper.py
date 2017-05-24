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
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
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
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
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
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
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
            conn = MySQLConnection(host='localhost', database='mysql', user='root', password='password')
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


psc = PyScrapper()
state = raw_input("\nPlease Enter the US state code to process:\n")
limit = int(raw_input("\nPlease Enter the limit:(Ideal value 1-15)\n"))
urllist = psc.get_baseurl_from_db(state,limit)
for url in range(len(urllist)):
    final_link= psc.prepare_url_link(urllist[url])
    scrapped_link = psc.fetch_already_scrap_link(state)

    for link in range(len(final_link)):
        if final_link[link] not in scrapped_link:
            company_info_list = psc.read_data_from_url(final_link[link],state)
            for company_info in range(len(company_info_list)):
                for indiv_company in range(len(company_info_list[company_info])):
                    psc.insert_data_into_company_table(company_info_list[company_info][indiv_company])
        else:
            print 'Link already scrapped!!!'
