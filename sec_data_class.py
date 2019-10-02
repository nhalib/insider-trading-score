#version : 1
#author : Nikhil N
#SCRY internal use only

#download financial statements and save temp to local directory

# Subject to modifications ;Lookout for changes in formatting of index page
# f4,13f,10Q
# SEC DATA extraction class


from req_py_libraries import *


class sec_data():

    def __init__(self):
        self.base_url = "https://www.sec.gov"


    def finstatement_urls(self,cik):
            link = "/Archives/edgar/data/" +cik+ "/"
            return(link)


    def main_filed_page(self,cik,acc_num): # main page of filed_document
            p1 = "https://www.sec.gov/Archives/edgar/data/"
            p2 = cik
            p3 = "/"
            p4 = acc_num.replace("_","")
            p5 = "/"
            p6 = acc_num
            p7 ="-index.htm"
            main_page = p1 + p2 + p3 + p4 + p5 + p6 + p7
            return(main_page)



    def url_response(self,url):
        boolman = False
        try:
            http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())  # Certified Websites filter
            response = http.request('GET', url)
            if (str(response.getheader('Content-Type')).find('html') >= 0):
                boolman = True
        except ValueError:
            response = 'Error 101.Page not readable'
        return [boolman, response]

    def feed_to_scdb(self,link,cursor,code):
        cmnd = "INSERT INTO process_sc (exch_tick,link,stat) VALUES ({0},{1},{2})".format("'"+str(code)+"'","'"+str(link)+"'",0)
        cursor.execute(cmnd)

    def feed_to_10qdb(self,link,cursor):
        cmnd = "INSERT INTO process_10q_dev (link,stat) VALUES ({0},{1})".format("'"+str(link)+"'",0)
        cursor.execute(cmnd)

    def feed_to_f4db(self,link,cursor):
        cmnd = "INSERT INTO process_f4 (link,stat) VALUES ({0},{1})".format("'" + str(link) + "'", 0)
        cursor.execute(cmnd)

    def feed_to_f4_insiderdb(self,datef,link,cursor):
        cmnd = "INSERT INTO process_f4_insider_learn (file_date,link,stat) VALUES ({0},{1},{2})".format("'"+str(datef)+"'","'" + str(link) + "'", 0)
        cursor.execute(cmnd)

    def feed_to_f13db(self,link,cursor):
        cmnd = "INSERT INTO process_13hr (link,stat) VALUES ({0},{1})".format("'" + str(link) + "'", 0)
        cursor.execute(cmnd)

    def process_SC(self,response,cursor,code,flink):
            response = response.find("table",{"summary":"Document Format Files"})
            try:
                response = response.find("td",string="SC 13G/A").parent
                link = self.base_url + response.find("a")['href']
                self.feed_to_scdb(link,cursor,code)
            except:
                try:
                    response = response.find("td", string="SC 13G").parent
                    link = self.base_url + response.find("a")['href']
                    self.feed_to_scdb(link, cursor, code)
                except:
                    print("SC 13G/13G/A cannot be processed",flink)

    def process_10Q(self,flink,cursor):
        url = flink
        flag,response = self.url_response(url)
        if(flag):
            response = BeautifulSoup(response.data, "html.parser")
            response = response.find("table",{"summary":"Data Files"})
            try:
                response = response.find("td",string="EX-101.INS").parent
                link = self.base_url + response.find("a")['href']
                self.feed_to_10qdb(link,cursor)
                #print(link)
            except:
                try:
                    response = response.find("td", string="XML").parent
                    link = self.base_url + response.find("a")['href']
                    self.feed_to_10qdb(link, cursor)
                except:
                    print("10-Q/K cannot be processed")

    def process_F4(self,flink,cursor):
        url = flink
        flag,response = self.url_response(url)
        if(flag):
            response = BeautifulSoup(response.data, "html.parser")
            response = response.find("table",{"summary":"Document Format Files"})
            responses = response.find_all("a")
            for resp in responses:
                text = resp.get_text()
                if(text.endswith(".xml")):
                    link = self.base_url + resp['href']
                    break
            try:
                self.feed_to_f4db(link,cursor)
            except:
                print('F4 cannot be processed')

    def process_F4_insider(self,datef,flink,cursor):
        url = flink
        flag,response = self.url_response(url)
        if(flag):
            response = BeautifulSoup(response.data, "html.parser")
            response = response.find("table",{"summary":"Document Format Files"})
            responses = response.find_all("a")
            for resp in responses:
                text = resp.get_text()
                if(text.endswith(".xml")):
                    link = self.base_url + resp['href']
                    break
            try:
                self.feed_to_f4_insiderdb(datef,link,cursor)
            except:
                print('F4 cannot be processed')


    def process_13F(self,flink,cursor):
        url = flink
        flag,responseu = self.url_response(url)
        link = ''
        if(flag):
            response1 = BeautifulSoup(responseu.data, "html.parser")
            response1 = response1.find("table",{"summary":"Document Format Files"})
            responses = response1.find_all("td", string="INFORMATION TABLE")
            for response in responses:
                responsel = response.parent
                link_responses = responsel.find_all("a")
                for resp in link_responses:
                    text = resp.get_text()
                    if (text.endswith(".xml")):
                        link = self.base_url + resp['href']
                        break
            response1 = BeautifulSoup(responseu.data, "html.parser")
            response1 = response1.find("table", {"summary": "Document Format Files"})
            responses = response1.find_all("td", string="13F-HR")
            for response in responses:
                responsel = response.parent
                link_responses = responsel.find_all("a")
                for resp in link_responses:
                    text = resp.get_text()
                    if (text.endswith(".xml")):
                        link = link + ';'+self.base_url + resp['href']
                        break
            try:
                self.feed_to_f13db(link,cursor)

            except:
                print('13F-HR cannot be processed')


new_feed_link = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start=0&count=40&output=atom"
