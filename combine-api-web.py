from serpapi import GoogleSearch
import json
from bs4 import BeautifulSoup
import requests
import secrets 
import sqlite3
import time




#creat database

DBNAME = 'FINAL507.sqlite'
connection = sqlite3.connect(DBNAME)

try:
    cursor = connection.cursor()
    query = "CREATE TABLE JOB_FINAL (id int, position varchar(255), company varchar(255), location varchar(255))"
    result = cursor.execute(query).fetchall()
    
    query = "CREATE TABLE HIB1 (company_name varchar(225), time varchar(255), status varchar(255),job_title varchar(225), salary varchar(255))"
    result = cursor.execute(query).fetchall()
    connection.close()
    
except:
    pass

# set the default value
query = 'data analysis' #dafaut input query positon is da

#input_value = input('Please input job title')


CACHE_FILE_NAME = 'job-h1b-checking.json'


def load_cache():
    with open(CACHE_FILE_NAME, mode='r', encoding='utf-8') as f:
        dicts = json.load(f)
        rr = []
        for i in dicts:
            rr.append(i)
    return rr

    
def save_cache(results):
    cache_file = open(CACHE_FILE_NAME,'w')
    contents_to_write = json.dumps(results)
    cache_file.write(contents_to_write)
    cache_file.close()
    
  
def make_url_request_using_cache(job_query): 
    try:
        results = []
        for i in list(range(0,110,10)): #search 200 results from API
            params = {
                "engine": "google_jobs",
                "q": job_query,
                "hl": "en",
                "api_key": "a463df1e2c78e577d9220ceeba3d0f6cc418db1a445ed7520d0fc6b0c62ab95a",
                "start":i
                }
            client = GoogleSearch(params)
            result = client.get_dict()
            result = result['jobs_results']      
            for i in result:    
                dic = {}
                dic['title'] = i['title']
                dic['company_name'] = i['company_name']
                dic['location'] = i['location']         
                results.append(dic)
        return results
    except:
        return False #if fail to finish search, return false


def fetching_caching_database_api(results):
    cache_file = load_cache()
    m = 0 
    r = 0
    l=0
    p=0

    for j in results:
        if j in cache_file:
            m+=1
        else:
            r+=1
            cache_file.append(j)
            save_cache(cache_file)

            DBNAME = 'FINAL507.sqlite'    
            connection = sqlite3.connect(DBNAME,timeout=10)
            query1 = 'select max(id) from JOB_FINAL'
            cursor = connection.cursor()
            id_tuple= cursor.execute(query1).fetchall()
            id_max = id_tuple[0][0]
            connection.close()
            try:
                id_max +=1
            except:
                id_max = 1
                    
            DBNAME = 'FINAL507.sqlite'    
            con = sqlite3.connect(DBNAME)
            cur = con.cursor()
            sql = 'insert into JOB_FINAL(id,position,company,location ) values(?,?,?,?)'      
            try:
                cur.execute(sql,(id_max,j['title'],j['company_name'],j['location'])) #插入一条数据
                con.commit()
                        #if l == 0:
                        #    print("插入数据成功")
                        #    l+=1
                l+=1
            except Exception as e:
                print(e)
                con.rollback()
                        #if p==0:
                        #    print("插入数据失败")
                        #    p+=1
                p+=1
            finally:
                cur.close()
    if r == 0 :
        print('caching exsting job information from database...')
    else:
        #print('caching ' + str(m) + ' exsting data from databases')
        print('fetching and storing ' + str(r) + ' new job data...')



CACHE_FILE_NAME1 = 'h1b.json'


def load_cache1():
    with open(CACHE_FILE_NAME1, mode='r', encoding='utf-8') as f:
        dicts = json.load(f)
        rr = []
        for i in dicts:
            rr.append(i)
    return rr

    
def save_cache1(results):
    cache_file = open(CACHE_FILE_NAME1,'w')
    contents_to_write = json.dumps(results)
    cache_file.write(contents_to_write)
    cache_file.close()
    
def fectching_data_from_web_database(results):
    w = 0
    q = 0
    for i in results:
        #try:
            company_name = i['company_name']
            #print(company_name)
            title = i['title']
            response = requests.get('http://visadoor.com/h1b/index?company=' +company_name + '&job=' +title +  '&state=&year=2020&submit=Search')   
            #soup = BeautifulSoup(response.text)
            
            soup = BeautifulSoup(response.text,"lxml")
            #soup = BeautifulSoup(response,"html.parser")


            a = soup.find('table',class_ = 'table table-bordered table-striped table-hover')
            if a is None:
                #print('false')
                #print(i)
                fajs = 1
            else:
                l = a.find_all('tr')
                #li = []
                for m in l[1:]:
                    s = {}
                    r = m.find_all('td')
                    s['company'] = company_name
                    s["time"] = r[1].text
                    s["status"] = r[3].text
                    s["job-title"] = r[-2].text
                    s["salary"] = r[-1].text  
                #print(s)
                    #print('succss')
                    #print(i)
                
        
                    cache_file1 = load_cache1()
                    if s in cache_file1:
                        w += 1
                    #print('caching')
                    else:
                        q += 1
                    #print('fetching')
                        cache_file1.append(s)
                        save_cache1(cache_file1)
                    
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()

                    #company_name varchar(225), time varchar(255), status varchar(255),job_title varchar(225), salary varchar(255)
                        sql = 'insert into HIB1(company_name,time,status,job_title, salary) values(?,?,?,?,?)'      
                        try:
                            cur.execute(sql,(s['company'],s['time'],s['status'],s['job-title'],s['salary']))
                            con.commit()
                        #if l == 0:
                            #print("插入数据成功")
                        #    l+=1
                        except Exception as e:
                            #print(e)
                            con.rollback()
                        finally:
                            cur.close()
        #except:
          #  o = 1
    if q ==0:
        print('caching exsting h1b information from database... ')
    else:
        #print('caching ' + str(w) + ' exsting h1b data from database...')
        print('fetching and storing ' + str(q) + ' new h1b data...')
        
        
        
def combine_api_web_db_json(query):
    results = make_url_request_using_cache(query)
    fetching_caching_database_api(results)
    fectching_data_from_web_database(results)
    combine_two_table()
    combine_two_table_inner()
    
    
    
    
    
    
def combine_two_table(): #left join保留全部的
    try:
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'DROP TABLE combine'
        result = cur.execute(query).fetchall()
        connection.close()
        
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'CREATE TABLE combine as select id, position, company, location, status,job_title, salary from JOB_FINAL a left join HIB1 b on a.company =  b.company_name and a.position = b.job_title   '
        result = cur.execute(query).fetchall()
        connection.close()
    except:
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'CREATE TABLE combine as select id, position, company, location, status,job_title, salary from JOB_FINAL a left join HIB1 b on a.company =  b.company_name and a.position = b.job_title   '
        result = cur.execute(query).fetchall()
        connection.close()
        
def combine_two_table_inner(): #inner join仅保留有的
    try:
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'DROP TABLE combine_inner'
        result = cur.execute(query).fetchall()
        connection.close()
        
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'CREATE TABLE combine_inner as select id, position, company, location, status,job_title, salary from JOB_FINAL a inner join HIB1 b on a.company =  b.company_name and a.position = b.job_title   '
        result = cur.execute(query).fetchall()
        connection.close()
    except:
        DBNAME = 'FINAL507.sqlite'    
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        query = 'CREATE TABLE combine_inner as select id, position, company, location, status,job_title, salary from JOB_FINAL a inner join HIB1 b on a.company =  b.company_name and a.position = b.job_title   '
        result = cur.execute(query).fetchall()
        connection.close()


    
def interactive_prompt():
    r = []
    while 'exit' not in r:
        job_position = input ('job position: ')
        if job_position == 'exit':
            break
        company1 = input('company: ')
        if company1 == 'exit':
            break
        location1 = input('location: ')
        if location1 =='exit':
            break
        h1b = input('Whether to filter h1b job position? ')
        if h1b == 'exit':
            break
        try:
            combine_api_web_db_json(job_position)
            if h1b.lower() == 'no':
                if company1 == '':
                    if location1 == '':
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine where position like "%' + job_position + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                    else:
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine where position like "%' + job_position + '%" and location like "%' + location1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                else:
                    if location1 == '':
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine where position like "%' + job_position + '%" and company like "%' + company1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                    else:
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine where position like "%' + job_position + '%" and location like "%' + location1 +  '%" and company like "%' + company1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                a = 0
                while a == 0:
                    if result ==[]:
                        print('Sorry, no corresponding position can be found, please try again... ')
                        break
                    else:
                        row_bars = '| {a} | {b} | {c} | {f} |'.format
                    
                    #row = "| {player:<16s} | {reb:2d} | {ast:2d} | {pts:2d} |".format
                        k = 1
                        dic = {}
                        l_repeat = []
                        l_filter = []
                        k_l = []
                        for j in result:
                            rep = []
                    
                            pos = j[1]
                            com = j[2]
                            loc = j[3]
                        
                        
                            rep.append(pos)
                            rep.append(com)
                            rep.append(loc)
                        
                            if rep in l_repeat:
                                pass
                            else:
                                l_repeat.append(rep)
                                l_filter.append(j)
                            
                        for j in l_filter:
                            k_l.append(str(k))
                            dic[str(k)] = j
                            id_1 = str(k)
                            pos = j[1]
                            if len(pos)<40:
                                pos = pos + (40-len(pos)) * ' '
                            else:
                                pass
                            com = j[2]
                            loc = j[3]
                            if k == 1:
                               # time.sleep(3)
                                print(row_bars(a='id',b ='position'+(40-len('position'))*' ', c = 'company',f = 'location'))
                                print(row_bars(a=id_1,b =pos, c = com,f = loc))
                            else:
                                ##time.sleep(3)
                                print(row_bars(a=id_1,b =pos, c = com,f = loc))
                            k+=1

                        choose_id = input('Select the id of position to know more about h1b information, or input "back" return previous page, or input "exit" to exit the program:')
                        if choose_id == 'back':
                            break
                        #pass
                        elif choose_id == 'exit':
                            r.append('exit')
                            return r
                        #pass
                        elif choose_id in k_l:
                            comp = dic[choose_id]
                            job_pos = comp[1]
                            if len(job_pos) < 40:
                                job_pos = job_pos + (40-len(job_pos)) * ' '
                            job_com = comp[2]
                            job_loc = comp[3]
                            job_h1b_status = comp[4]
                            job_h1b_salary = comp[6]
                            g = []
                            while 'break' not in g:
                                if job_h1b_status == None:
                                    print('Sorry, we cannot found the h1b information of this job position..' + '\n' * 1)
                                else:
                                    row_bars = '| {a} | {b} | {c} | {d} | {e} | {f} |'.format
                                    print(row_bars(a='id',b ='position'+(40-len('position'))*' ', c = 'company',d = 'location',e = 'h1b status',f = 'salary'))
                                    print(row_bars(a=choose_id,b =job_pos, c = job_com,d = job_loc,e = job_h1b_status,f = job_h1b_salary)+ '\n' * 1)
                                br = input ('Please input "back" return previous page, or input "exit" to exit the program:')
                                if br == 'back':
                                    break
                                elif br == 'exit':
                                    r.append('exit')
                                    return r
                                else:
                                    print('sorry,the input is invalid, please input "back" return previous page, or input "exit" to exit the program:')
                        else:
                            print('sorry, the input is invalid, please input again')

            elif h1b.lower() == 'yes':
                if company1 == '':
                    if location1 == '':
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine_inner where position like "%' + job_position + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                    else:
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine_inner where position like "%' + job_position + '%" and location like "%' + location1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                else:
                    if location1 == '':
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine_inner where position like "%' + job_position + '%" and company like "%' + company1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                    else:
                        DBNAME = 'FINAL507.sqlite'    
                        con = sqlite3.connect(DBNAME)
                        cur = con.cursor()
                        query = 'select * from combine_inner where position like "%' + job_position + '%" and location like "%' + location1 +  '%" and company like "%' + company1 + '%"'
                        result = cur.execute(query).fetchall()
                        connection.close()
                a = 0
                while a == 0:
                    if result ==[]:
                        print('Sorry, no corresponding position can be found, please try again... ')
                        break
                    else:
                        row_bars = '| {a} | {b} | {c} | {f} |{g} |{h} |'.format
                    
                    #row = "| {player:<16s} | {reb:2d} | {ast:2d} | {pts:2d} |".format
                        k = 1
                        dic = {}
                        l_repeat = []
                        l_filter = []
                        k_l = []
                        for j in result:
                            rep = []
                    
                            pos = j[1]
                            com = j[2]
                            loc = j[3]
                            status = j[4]
                            salary = j[6]

                            rep.append(pos)
                            rep.append(com)
                            rep.append(loc)
                            rep.append(status)
                            rep.append(salary)
                        
                            if rep in l_repeat:
                                pass
                            else:
                                l_repeat.append(rep)
                                l_filter.append(j)
                            
                        for j in l_filter:
                            k_l.append(str(k))
                            dic[str(k)] = j
                            id_1 = str(k)
                            pos = j[1]
                            if len(pos)<40:
                                pos = pos + (40-len(pos)) * ' '
                            else:
                                pass
                            com = j[2]
                            loc = j[3]
                            status = j[4]
                            salary = j[6]

                            if k == 1:
                                time.sleep(3)
                                print(row_bars(a='id',b ='position'+(40-len('position'))*' ', c = 'company',f = 'location',g = 'status',h = 'salary'))
                                print(row_bars(a=id_1,b =pos, c = com,f = loc,g = status,h = salary))
                            else:
                               ##time.sleep(3)
                                print(row_bars(a=id_1,b =pos, c = com,f = loc,g = status,h = salary))
                            k+= 1
                    choose_id = input('Please input "back" return previous page, or input "exit" to exit the program:')
                    if choose_id == 'back':
                        break
                    elif choose_id == 'exit':
                        r.append('exit')
                        return r 
            else:
                print('Please input valid value..')
        except:
            print('Sorry we can not find the information..Please input again')
            
            
if __name__=="__main__":
 interactive_prompt()             
  