#!/usr/bin/env python

# Batch export of all docs from a Cloudant DB to a CSV file in Swift
# Uses DLO support for parallel upload to increase speed
# Takes stored environment variables for Swift and Cloudant auth data

# Author: Brad Bonn - bbonn@us.ibm.com

import logging
from cloudant.account import Cloudant
from cloudant import cloudant
import os
import json
from swiftclient import service
from swiftclient import client

def configuration():
    container_name = 'cloudant-export'
    import argparse
    argparser = argparse.ArgumentParser(description = 'Tool to export high-volume data from Cloudant into flat CSV files in Swift')
    argparser.add_argument(
        'account',
        type=str,
        help = 'Cloudant account name'
    )
    argparser.add_argument(
        'database',
        type=str,
        help = 'Cloudant database name'
    )
    argparser.add_argument(
        'key',
        type=str,
        help = "field name that identifies a document's type, such as 'type'"
    )
    argparser.add_argument(
        'value',
        type=str,
        help = 'value of the previous field that identifies the document type whose schema we will use'
    )
    argparser.add_argument(
        '-c',
        metavar='container name',
        type=str,
        nargs='?',
        help='Swift container for uploading to. Uses {0} by default'.format(container_name),
        default = container_name
        )
    argparser.add_argument(
        '-s',
        metavar = 'segment size (MB)',
        type = int,
        nargs = '?',
        help = "Size of each DLO segment in megabytes. Defaults to 2048.",
        default = 2048
    )
    argparser.add_argument(
        '-p',
        metavar = 'process count',
        type = int,
        nargs = '?',
        help = "Number of parallel processes to run. Defaults to 4.",
        default = 4
    )
    myargs = argparser.parse_args()
    
    config = dict(
        container = myargs.c,
        segment_size = myargs.s,
        tag = dict(
            key = myargs.key,
            value = myargs.value
        ),
        database = myargs.database,
        pool_size = myargs.p,
        swift = dict(
            user = os.environ.get('OS_USER_ID'),
            password = os.environ.get('OS_PASSWORD'),
            auth_url = os.environ.get('OS_AUTH_URL')
        ),
        cloudant = dict(
            user = os.environ.get('CLOUDANT_USER'),
            password = os.environ.get('CLOUDANT_PASS')
        )
    )
    
    return config

def main():
    # Process arguments and get configuration settings
    config = configuration()
    # Field that identifies the docs we want
    document_tag = {config['tag']['key']: config['tag']['value']}
    # Import environment auth variables
    swift = dict(
        swift_user = os.environ.get('OS_USER_ID'),
        swift_pass = os.environ.get('OS_PASSWORD'),
        swift_auth = os.environ.get('OS_AUTH_URL')
    )
    cloudant_user = os.environ.get('CLOUDANT_USER')
    cloudant_pass = os.environ.get('CLOUDANT_PASS')

    # Create the container
    swiftconn.put_container(config['container'])
    # Open up Cloudant connection
    with Cloudant(cloudant_auth['user'],cloudant_auth['password'],account=config['account']) as client:
        db = client[config['database']]
        # Obtain JSON format based on first doc of desired format
        for doc in db.all_docs(descending=True,limit=100,skip=100,include_docs=True)['rows']:
            try:
                if doc['doc'][config['tag']['key']] == config['tag']['value']:
                    # Found the right doc type
                    pass
            except:
                continue
    
def transfer_segment(db,range):
    # Open swift connection
    swiftconn = swiftclient.Connection(
        user = swift['swift_user'],
        key = swift['swift_pass'],
        authurl = swift['swift_auth']
    )
    
def testswift():
    try:
        data = open('testfile.html')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        sys.exit(2)
    
    # Open swift connection
    swiftconn = client.Connection(
        user = config['swift']['user'],
        key = config['swift']['password'],
        authurl = config['swift']['auth_url']
    )
    try:
        swiftconn.http_connection(url=config['swift']['auth_url'])
        print swiftconn.get_capabilities()
        #container = swiftconn.put_container('testy')
        #swiftconn.put_object('testy',data)
        swiftconn.close()
    except client.ClientException as e:
        print "{0}".format(e)
    except Exception as e:
        print e


    
config = configuration()
testswift()

#    
#if __name__ == '__main__':
#    pass