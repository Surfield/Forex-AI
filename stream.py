"""
Demonstrates streaming feature in OANDA open api
To execute, run the following command:
python streaming.py [options]
To show heartbeat, replace [options] by -b or --displayHeartBeat
"""
import v20
import requests
import json

from optparse import OptionParser
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_db(db_name):
    # For local use
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db
    
db = get_db('oanda')

def connect_to_stream():
    """
    Environment           <Domain>
    fxTrade               stream-fxtrade.oanda.com
    fxTrade Practice      stream-fxpractice.oanda.com
    sandbox               stream-sandbox.oanda.com
    
    curl -i  -H "Authorization: Bearer "   
    """

    # Replace the following variables with your personal ones
    domain = 'stream-fxtrade.oanda.com'
    access_token = 
    account_id = 
    instruments = "EUR_USD%2CUSD_CAD%2CGBP_USD%2CUSD_JPY%2CAUD_USD%2CNZD_USD" #"EUR_USD,USD_CAD"

    try:
        s = requests.Session()
        #url = "https://" + domain + "/v3/prices"
        url = "https://" + domain + "/v3/accounts/"+ account_id +"/pricing/stream?instruments="+instruments 
        headers = {'Authorization' : 'Bearer ' + access_token,
                   # 'X-Accept-Datetime-Format' : 'unix'
                  }
        #params = {'instruments' : instruments, 'accountId' : account_id}
        req = requests.Request('GET', url, headers = headers)#, params = params)
        pre = req.prepare()
        resp = s.send(pre, stream = True, verify = False)
        return resp
    except Exception as e:
        s.close()
        print("Caught exception when connecting to stream\n" + str(e)) 

def demo(displayHeartbeat):
    response = connect_to_stream()
    if response.status_code != 200:
        print(response.text)
        return
    for line in response.iter_lines(1):
        if line:
            try:
                msg = json.loads(line)
            except Exception as e:
                print("Caught exception when converting message into json\n" + str(e))
                return
            
            if displayHeartbeat:
                print(line)
            else:
                if "instrument" in msg or "tick" in msg:
                    # db.forex.insert(msg)# insert_ones
                    print(msg)

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-b", "--displayHeartBeat", dest = "verbose", action = "store_true", 
                        help = "Display HeartBeat in streaming data")
    displayHeartbeat = False

    (options, args) = parser.parse_args()
    if len(args) > 1:
        parser.error("incorrect number of arguments")
    if options.verbose:
        displayHeartbeat = True
    demo(displayHeartbeat)


if __name__ == "__main__":
    main()