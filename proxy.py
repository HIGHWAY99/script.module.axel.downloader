'''
    AxelProxy XBMC Addon
    Copyright (C) 2013 Eldorado

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA 02110-1301, USA.
'''

import re
import urllib2
import sys
import traceback
import socket
import base64
import hashlib
import os
import time
import threading
from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import multiprocessing
import axel
import common


http_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; '
        'en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,'
        'text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'en-us,en;q=0.5',
    }
    
class MyHandler(BaseHTTPRequestHandler):


    #Handles a HEAD request
    def do_HEAD(self):
        self.send_response(200)
        rtype="video/mp4"
        #(file_url,file_name)=self.decode_B64_url(self.path)
        #print 'file_url
        #file_size,file_url,rtype=self.get_file_size(file_url)# get the url again, incase there is a redirector
        self.send_header("Content-Type", rtype)
        #self.send_header("Content-Length", file_size)
        self.send_header("Accept-Ranges","bytes")

        self.end_headers()
        # Only send the head #we should forward this
 
        #(file_url,file_name)=self.decode_B64_url(request_path)
        ##file_size,file_url=self.get_file_size(file_url)# get the url again, need redirection implementation
        #rtype="application/x-msvideo"
        #etag=self.generate_ETag(file_url)
        #self.send_http_headers(file_name, rtype, file_size , etag)
        #print "HEAD request"

    #Handles a GET request.
    def do_GET(self):
        print "GET request"
        # Send head and video
        self.answer_request()


    #Handle incoming requests
    def answer_request(self):
        try:

            #Pull apart request path
            request_path=self.path[1:]       
            request_path=re.sub(r"\?.*","",request_path)
            #If a request to stop is sent, shut down the proxy
            if request_path.lower()=="stop":
                sys.exit()
            if request_path.lower()=="status":
                print 'Get STatus Call!'
                self.respondStatus();
                self.wfile.close()
                return
            if  request_path.lower()=='stopdownload' :
                file_stop=re.findall( '=(.*)',self.path[1:])[0]
                print 'Get stop download Call!',request_path,file_stop
                self.respondStopDownload(file_stop);
                self.wfile.close()
                return

            #If a range was sent in with the header
            requested_range=self.headers.getheader("Range")

            print 'REQUEST PATH: %s' % request_path
            print 'REQUEST RANGE: %s' % requested_range

            #Expecting url to be sent in base64 encoded - saves any url issues with XBMC
            (file_url,file_name)=self.decode_B64_url(request_path)


            #Send file request
            self.handle_send_request(file_url, file_name, requested_range)
        


        except:
            #Print out a stack trace
            traceback.print_exc()

            #Close output stream file
            self.wfile.close()
            return

        #Close output stream file
        self.wfile.close()
        return

    def respondStopDownload(self,filename):
        #response='Stopping',filename#self.getStatus()
        response=self.stopDownlading(filename)
        if response==True:
            response="Termination has been Queued!"

        print response
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.send_header('Content-length', len(response))
        self.end_headers()
        self.wfile.write(response)

    def stopDownlading(self,filename):
        #response='Stopping',filename#self.getStatus()
        print 'stopping',filename
        cached = self.get_from_cache(filename)   
        if cached:
            (file_size, file_name,downloader,writeAccess) = cached
            downloader.terminate();
            if self.remove_from_cache(filename):
                response=True
        else:
            response="file not found"
        
        return response

    def respondStatus(self):
        response=self.getStatus()
        print response
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.send_header('Content-length', len(response))
        self.end_headers()
        self.wfile.write(response)


    def getStatus(self):
        global file_cache
        try:
            if len(file_cache):
                htmlText="<html><head></head><body>"
                htmlText+="<table width=100pc>"
                htmlText+= "<TR>"
                htmlText+= "<TD>Action</TD>"
                htmlText+= "<TD>File Name </TD>"
                htmlText+= "<TD>File Size (MB) (bytes)</TD>"
                htmlText+= "<TD>Completed</TD>"
                htmlText+= "<TD>Terminated</TD>"
                htmlText+= "<TD>Chunks Size (KB)</TD>"
                htmlText+= "<TD>Total Chunks</TD>"
                htmlText+= "<TD>Total Chunks Completed</TD>"
                htmlText+= "<TD>Total Chunks Remaining</TD>"
                htmlText+= "</TR>"
                #if 1==2:
                for fn in file_cache:
                    file_size, file_name,downloader,writeAccess=file_cache[fn]
                    htmlText+= "<TR>"
                    htmlText+= "<TD><a href=\"http://127.0.0.1:" +str(PORT_NUMBER)+ "/StopDownload?fname="+ file_name + "\">Stop</a></TD>"
                    htmlText+= "<TD>"+ file_name+"</TD>"
                    htmlText+= "<TD>"+ str(int(file_size)/1024/1024 )+"</TD>"
                    htmlText+= "<TD>"+ str(downloader.completed) +"</TD>"
                    htmlText+= "<TD>"+ str(downloader.terminated) +"</TD>"
                    htmlText+= "<TD>"+ str(downloader.chunk_size/1024) +"</TD>"
                    htmlText+= "<TD>"+ str(downloader.total_chunks) +"</TD>"
                    htmlText+= "<TD>"+ str(len(downloader.completed_work())) +"</TD>"
                    htmlText+= "<TD>"+ str(downloader.total_chunks-len(downloader.completed_work())) +"</TD>"
                    htmlText+= "</TR>"
                    htmlText+= "<TR>"
                    htmlText+= "<TD colspan=9>"
                    htmlText+= "<table><TR>"
                    L=downloader.completed_work()
                    print L
                    
                    for index in range(0,downloader.total_chunks):
                        cellW=100/downloader.total_chunks
                        s_number=index*downloader.chunk_size
                        if len(L):
                            if  len([b for b,s in enumerate(L) if s[1] == s_number])>0:
                                htmlText+= "<TD bgcolor=green width=" + str(int(cellW)) + "pc>|</TD>"
                            else:
                                htmlText+= "<TD bgcolor=red width=" + str(int(cellW)) + "pc>|</TD>"
                    htmlText+= "</TR></table>"
                    htmlText+= "</TD></TR>"

                htmlText+="</table>"
                htmlText+="</body></html>" 
                print htmlText
                return htmlText
            else:
                return 'Nothing in cache/downloading'
        except Exception, e:
            print 'Exception creating status: %s' % e
            return 'Error in status' #connection drop
    
    def handle_send_request(self, file_url, file_name, s_range):

        #Check if file has been cached yet
        # - Grab file info if it has
        # - Else Save file info to cache
        file_dest = common.profile_path
        
        cached = self.get_from_cache(file_name)
        downloader=None
        rtype="video/mp4"
        if cached:
            (file_size, file_name,downloader,writeAccess) = cached
            print ' got it from cache'
        else:
            print ' NOE create it'
            file_size,file_url,rtype=self.get_file_size(file_url)# get the url again, incase there is a redirector
            file_size=int(file_size)
            import axel
            downloader = axel.AxelDownloader() # store in the same variable
            writeAccess = multiprocessing.Condition()
            self.save_to_cache(file_name, (file_size, file_name,downloader,writeAccess))

        (srange, erange) = self.get_range_request(s_range, file_size)
        
        print 'REQUESTING from %s to %s, srange=%s' % (str(srange), str(erange),s_range)
        


        #Set response type values
        
        etag=self.generate_ETag(file_url)

        content_size= file_size
        videoContents=""
        # Do we have to send a normal response or a range response?
        portionLen=0
        if s_range and not s_range=="bytes=0-0":
            self.send_response(206)
            videoContents,portionLen=self.get_video_portion( file_url, file_dest, file_name, srange,erange,downloader)
            #crange="bytes "+str(srange)+"-" +str(int(srange+portionLen)-1)+"/"+str(content_size)#recalculate crange based on srange, portionLen and content_size 
            crange="bytes "+str(srange)+"-" +str(int(content_size-1))+"/"+str(content_size)#recalculate crange based on srange, portionLen and content_size 
            self.send_header("Content-Range",crange)
            #content_size=#content_size#portionLen; #we are sending a portion so send correct info
        else:
            #Send back 200 reponse - OK
            self.send_response(200)
            self.send_header("Accept-Ranges","bytes")


        self.send_http_headers(file_name, rtype, content_size , etag)
  
        if portionLen>0:
            
            dataSent=self.send_video_content(self.wfile, videoContents)
            try:
                while dataSent:
                    srange+=portionLen;
                    videoContents,portionLen=self.get_video_portion( file_url, file_dest, file_name, srange,content_size-1,downloader)
                    if portionLen==0:  
                        print 'no more'
                        break
                    dataSent=self.send_video_content(self.wfile, videoContents)
            except:
                print "Connection closed."
                return


    def send_video_content(self,file_out, videoData):
        try:
            file_out.write(videoData);
            file_out.flush();

            return True
        except Exception, e:
            print 'Exception sending video porting: %s' % e
            return False #connection drop

    def get_video_portion(self, file_link, file_dest, file_name, start_byte, end_byte, downloader):
        #print 'Starting download at byte: %d' % start_byte
        lenOfData=0
        full_path = os.path.join(common.profile_path, file_name)
        MAX_RETURN_LENGTH=1024*500; #500k
        if downloader.stopProcessing: return ""#stop by someone!
        if not downloader.started:
            #import axel
            #downloader = axel.AxelDownloader() # store in the same variable
            print 'Starting downloader 0 '
            dt = threading.Thread(target=downloader.download, args = (file_link, file_dest, file_name, start_byte))
            print 'Starting downloader '
            dt.start()
            time.sleep(10)# sleep till we get some data, this is first time only
        else:
            if not downloader.completed:
                if not downloader.anythingDownloadYet(start_byte):
                    downloader.repriotizeQueue(start_byte)#tell threads to move to this new location
                    time.sleep(5)# sleep till we get some data
                #else:
                #    print 'repriotizeQueue not required'


        fileContents=""
        try:
            #Opening file
            #print 'now checking'
            if (int(end_byte)-int(start_byte))>MAX_RETURN_LENGTH: # how much data asked by xbmc#too much? remember we are sending chunks
                end_byte = int(start_byte)+int(MAX_RETURN_LENGTH)-1;  

            #print 'start and endbyte', start_byte,end_byte,MAX_RETURN_LENGTH,end_byte-start_byte
            #print 'getting downloadedPortion'
            tries=0
            while not downloader.anythingDownloadYet(start_byte):
                time.sleep(2)
                tries+=1
                if tries>5: return ""# can't wait forever
            #print 'getting downloadedPortion end'
            fileContents,lenOfData=downloader.getDownloadedPortion(start_byte,end_byte)
            print 'getting downloadedPortion end',lenOfData 
        except Exception, e:
            print 'Exception sending file: %s' % e
            pass
        return fileContents,lenOfData;



    def get_file_size(self, url): #check the redirector here and update the url if needed
        request = urllib2.Request(url, None, http_headers)
        data = urllib2.urlopen(request)
        content_length = data.info()['Content-Length']
        content_type=data.info()['Content-Type']
        return content_length,url,content_type


    #Set and reply back standard set of headers including file information
    def send_http_headers(self, file_name, content_type, content_size , etag,):
        print "Sending headers"
        try:
            self.send_header("Content-Disposition", "inline; filename=\"" + file_name.encode('iso-8859-1', 'replace')+"\"")
        except:
            pass
        self.send_header("Content-Type", content_type)
        self.send_header("Last-Modified","Wed, 21 Feb 2000 08:43:39 GMT")
        self.send_header("ETag",etag)
        self.send_header("Accept-Ranges","bytes")
        self.send_header("Cache-Control","public, must-revalidate")
        self.send_header("Cache-Control","no-cache")
        self.send_header("Pragma","no-cache")
        self.send_header("features","seekable,stridable")
        self.send_header("client-id","12345")
        self.send_header("Content-Length", str(content_size))
        self.send_header("Connection", 'close')
        self.end_headers()


    #Generate a unique hash tag
    def generate_ETag(self, url):
        md=hashlib.md5()
        md.update(url)
        return md.hexdigest()


    def get_range_request(self, hrange, file_size):
        if hrange==None:
            srange=0
            erange=None
        else:
            try:
                #Get the byte value from the request string.
                hrange=str(hrange)
                splitRange=hrange.split("=")[1].split("-")
                srange=int(splitRange[0])
                erange = splitRange[1]
                if erange=="":
                    erange=int(file_size-1)
                #Build range string
                
            except:
                # Failure to build range string? Create a 0- range.
                srange=0
                erange=int(file_size-1);
        return (srange, erange)


    def decode_B64_url(self, b64):
        url = base64.b64decode(b64)
        file_name = url.split('/')[-1]
        file_name=file_name.split('?')[0]
        return (url, file_name )


    def get_from_cache(self, name):
        global file_cache
        try:
            return file_cache[name]
        except:
            return None
    def remove_from_cache(self, name):
        global file_cache
        try:
            del file_cache[name]
            return True
        except:
            return False


    def save_to_cache(self, name, details):
        global file_cache
        try:
            file_cache[name]=details
        except Exception, e:
            print 'Error attempting to save to cache: %s', e
            pass


class Server(HTTPServer):
    """HTTPServer class with timeout."""

    def get_request(self):
        """Get the request and client address from the socket."""
        # 10 second timeout
        self.socket.settimeout(5.0)
        result = None
        while result is None:
            try:
                result = self.socket.accept()
            except socket.timeout:
                pass
        # Reset timeout on the new socket
        result[0].settimeout(1000)
        return result

class ThreadedHTTPServer(ThreadingMixIn, Server):
    """Handle requests in a separate thread."""

#Address and IP for Proxy to listen on
HOST_NAME = '127.0.0.1'
#HOST_NAME = 'localhost'
PORT_NUMBER = 45550

#Init file_cache - stores file information for repeat requests
global file_cache
file_cache={}
print "AxelProxy Downloader getting Ready"

if __name__ == '__main__':  
    socket.setdefaulttimeout(10)
    server_class = ThreadedHTTPServer

    myhandler=MyHandler
    myhandler.protocol_version = "HTTP/1.1"
    httpd = server_class((HOST_NAME, PORT_NUMBER), myhandler)
    print "AxelProxy Downloader Starting - %s:%s" % (HOST_NAME, PORT_NUMBER)
    while(True):
        httpd.handle_request()
    httpd.server_close()
    print "AxelProxy Downloader Stopping %s:%s" % (HOST_NAME, PORT_NUMBER)
