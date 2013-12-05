#!/usr/bin/python
#coding = utf-8

import boto
import urllib2
import json
from boto.s3.connection import S3Connection
import urllib, MySQLdb
import sys

def usage():
    print "Usage: python26 mysql.py or ./mysql.py"
    sys.exit(1)

if len(sys.argv) != 1:
    usage()


def main():

    # Connect to the database and create the tables
    connSQL = MySQLdb.connect (host = "localhost",
                       user = "root",
                       passwd = "82267185", init_command = "set names utf8")
    connSQL.autocommit(False)
    cursor = connSQL.cursor ()
    cursor.execute("set names utf8")
    cursor.execute ("DROP DATABASE IF EXISTS twitter")
    cursor.execute ("CREATE DATABASE twitter")
    cursor.execute ("USE twitter")
    cursor.execute ("""
        CREATE TABLE tweetsq3
        (
          tid         BIGINT,
          userid      INT,
          create_at   VARCHAR(35),
          content     TEXT,
          o_userid    BIGINT
        ) ENGINE=InnoDB CHARACTER SET=utf8;
   """)
    
    conn =   S3Connection('AKIAIUUSLBP7GFHBNSDA','PDCZYJMlo6C4JnyWTMybv2XhDLbZhdtHsIjoTU20')
    mybucket = conn.get_bucket('sqltwitter')
    set = mybucket.list()
    for i in set:
            s=i.name
            s = "http://s3.amazonaws.com/sqltwitter/" + s
            response = urllib2.urlopen(s)
            tweets = response.read().split('\n')
            print s
            
            for tweetJasons in tweets:
                    tweetJason = tweetJasons.split('\t')[1]
                    if tweetJason == "":
                            continue
                    tweet = json.loads(tweetJason)
                    userid = tweet['userid']
	            print userid
                    tid = tweet['tid']
		    print tid 
                    content = tweet['text']
                    #content = "content"
	#	    print content
		    create_at = tweet['timestamp']
                    #if tweets['o_userid'] == false:
                    #    o_userid = -1
                    #else:
                    o_userid = tweet['o_userid']
 #                   print userid + ' '+ tid+ ' '+ content +' ' + create_at + '\n'   

                    cursor.execute ("""INSERT INTO tweets   (tid, userid, create_at, content, o_userid) VALUES  (%s, %s, %s, %s, %s)""", (tid, userid, create_at, content.encode("utf-8"), o_userid))
    #Commit the changes.
                    connSQL.commit()
		    print "my heart is in the work"  
    cursor.close()
    connSQL.close()
       
if __name__=="__main__":
    main()
