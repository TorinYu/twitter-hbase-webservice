#!/usr/bin/python
#coding = utf-8

import boto
import urllib2
import json
from boto.s3.connection import S3Connection
import urllib, MySQLdb
import sys


def main():

    conn =   S3Connection('AKIAIUUSLBP7GFHBNSDA','PDCZYJMlo6C4JnyWTMybv2XhDLbZhdtHsIjoTU20')
    mybucket = conn.get_bucket('tweetq2')
    set = mybucket.list()
    f = open('tweetsq2.csv','w')
    for i in set:
            s=i.name
            s = "http://s3.amazonaws.com/tweetq2/" + s
            response = urllib2.urlopen(s)
            tweets = response.read().split('\n')
            print s
            
            for tweetJasons in tweets:
                    if tweetJasons == "":
                            continue
                    
                    create_at = tweetJasons.split('\t')[0]
 		    
                    tid_textList = sorted(tweetJasons.split('\t')[1:])
                    tid_text = '\t'.join(tid_textList)
                    f.write(create_at + '\t\t' + tid_text +'\n')
       
if __name__=="__main__":
    main()
