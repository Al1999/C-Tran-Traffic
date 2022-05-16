import urllib3
from datetime import date
today = date. today()
d3 = today. strftime("%m-%d-%y")
name = d3 + '.json'
http = urllib3.PoolManager()
resp = http.request ("GET", 'http://www.psudataeng.com:8000/getBreadCrumbData')

file = open (name, 'w')
file.close
with open(name, 'b+w') as f:
    f.write (resp.data)


resp.release_conn()

#print (resp.data)
