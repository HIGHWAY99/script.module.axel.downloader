'''
    axel downloader XBMC Addon
    Copyright (C) 2013 Eldorado
    
    This class takes a given direct http file link and attempt to download in
    multiple connections as specified. Each connection runs under it's own thread, separate from the main task.
    
    File size is taken into consideration and split into logical chunks, downloaded in order of priority.
    
    File is written to disk according to priority. Writting is done in it's own single thread separate from main.
    
    HTTP error detection is included, most common errors will cause the chunk to be re-added back to the queue

    	- 503 error is interpreted as a connection denied indicating that we are trying to open too many than the host allows, 
    	  the chunk will be sent back into the queue and the thread will finish, reducing the number of running threads/connections by 1
    
      - socket time out error will cause chunk to be sent back into queue and retried   
    
    Created by: Eldorado
    
    Credits: Bstrdsmkr and the rest of the XBMCHub dev's
    
    
*To-Do:
-
-

'''

import Queue
import threading
import urllib2
import socket
import os
import multiprocessing
#from downloader import Downloader
import common #todo: remove this to make xbmc independent
from common import Singleton
import time
import datetime
import traceback
import sys





class AxelDownloadManager(Singleton):
    def __init__(self): 
        print 'init for AxelDownloadManager'# should happen only once
        self.downloads={}
        self.currentThread=None
        self.currentThread =threading.Thread(target=self.check_forzen_downloads, args = (5,)) #give eight seconds TODO, need better handleing at streaming stop
        self.currentThread.start()
        
    def check_forzen_downloads(self,wait_time):
        while True:
            time.sleep(5)# every 5 seconds, check
            downloaders = self.get_downloaders()
            to_delete=[]
            try:
                #print 'running check_forzen_downloads '
                if len(downloaders):
                    for downloader_name in downloaders:
                        downloader=downloaders[downloader_name]
                        #print 'time of chunk',downloader.time_of_chunk
                        #print  downloader.keep_file    ,downloader.started 
                        #print 'downloader.clients',downloader.clients
                        if downloader.download_mode==1 and downloader.started and not downloader.keep_file:#this is streaming
                            if downloader.clients==0:# (datetime.datetime.now()-downloader.time_of_chunk).seconds>wait_time:
                                print 'found stopped stream',downloader_name
                                to_delete.append(downloader_name)
                        if downloader.download_mode==2 and downloader.completed: #Todo: move to history so that it could be viewed etc
                            to_delete.append(downloader_name)
                for item_name in to_delete:
                    #print 'item_name',item_name
                    self.stop_downloader(item_name)
                                    
            except Exception, e:
                traceback.print_exc(file=sys.stdout)
                #print 'Failed in check_forzen_downloads  #%s :'%e 
            
    def get_downloaders(self):

        return  self.downloads
        
    #file_link, file_dest='', file_name='',start_byte=0
    def start_downloading(self,file_link, file_dest, file_name, start_byte,download_mode ,keep_file,connections):
        downloader=self.current_downloader(file_name)
        if downloader: #there is a download going
                #should we throw error or reuse that? probably filename is not unique? TODO
            downloader.clients+=1;
            print 'downloader already exists'
        else:
            downloader = AxelDownloader() # store in the same variable
            dt = threading.Thread(target=downloader.download, args = (file_link, file_dest, file_name, start_byte))
            print 'Starting downloader '
            dt.start()
            time.sleep(5)# todo. better handeling
            #store the currentdownload
            print 'downloader started',file_name
            self.store_downloader(file_name,downloader) #we better create a uniquekey based on url etc

        return downloader

    def stop_downloader(self,name):
        try:
            downloader=self.current_downloader(name)
            #print 'name',name
            if downloader: 
                #print 'terminating1'
                downloader.terminate(True);
                 
                #print 'terminating2'
                del self.downloads[name]
                #print 'terminating3'
                return True
            else:
                return False
            
        except Exception,e:
            print 'Failed in stop_downloader  #%s :'%e 
            return False
            
    def store_downloader(self,name,downloader):
        try:
            self.downloads[name]=downloader
            return True
        except Exception, e:
            print 'err  in store_downloader %s'%e
            return False
            
    def current_downloader(self,name):
        try:
            return self.downloads[name]
        except:
            return None
            

class AxelDownloader:

    '''
    This is the main class to import

        - 
    '''  

    def __init__(self, num_connections=2, chunk_size=1024*1024, keep_file=False, download_mode=1):#2000000
        '''
        Class init      
        
        Kwargs:
            num_connections (int): number of connections/threads to attempt to open for downloading
            chunk_size (int): size in bytes for each file 'chunk' to be downloaded per connection
        '''
        self.workQ = Queue.PriorityQueue()
        self.resultQ = Queue.PriorityQueue()
        self.currentThreads=[]
        self.completedWork=[]
        self.isAllowed = multiprocessing.Condition()
        self.saveFileLock = multiprocessing.Condition()
        self.stopEveryone=False;     
        self.clients=1
        #Class variables
        self.num_conn = num_connections
        self.chunk_size = chunk_size
        self.total_chunks = -1
        self.keep_file=keep_file
        self.download_mode=download_mode #1=stream, 2=download

        self.http_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; '
                'en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            'Accept': 'text/xml,application/xml,application/xhtml+xml,'
                'text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
            'Accept-Language': 'en-us,en;q=0.5',
        }
        self.completed=False
        self.fileFullPath=""; #init with blank
        self.started=False
        self.stopProcessing=False
        self.terminated=False
        self.fileLen=0
        self.filename =""
        #common.addon.log('Axel Downloader Intitialized')# not an xbmc but rather python downloader

    def get_video_chunk(self,start_byte, timeout, chunkSize =1024):

        self.time_of_chunk=datetime.datetime.now()
        #MAX_RETURN_LENGTH=chunkSize#1024*500; #500k# TODO, get this parameterized
        end_byte=start_byte + chunkSize-1;
        if end_byte>self.fileLen-1:
            end_byte=self.fileLen-1;
            
        if not self.started:
            time.sleep(5)# sleep for 5 seconds, this is so that queue is built
        elif not self.completed and len(self.completedWork)>0: #if download is not but done some download then this must be seek
            if not self.is_chunk_downloaded(start_byte): #chunk doesn't exits?, may be its a seek?
                    self.repriotize_queue(start_byte)#tell threads to move to this new location

        if start_byte>=self.fileLen-1:
            return "",0 #done here
        self.time_of_chunk=datetime.datetime.now()

        tries=0
        while not self.is_chunk_downloaded(start_byte): 
            time.sleep(2)
            tries+=1
            if tries>(timeout/2): return "",0# can't wait forever
        self.time_of_chunk=datetime.datetime.now()

        return self.get_downloaded_data_from(start_byte,end_byte)
    
    def completed_work(self):
        return self.completedWork
 
    def terminate(self, fromStreamer=False):
        self.stopProcessing=True
        for t,c in self.currentThreads:
            t.terminate()
        self.cleanup(fromStreamer)

    def is_chunk_downloaded(self, start_byte): #tell us if anything exists for given starting point
        sIndex=-1;
        for i, item in enumerate(self.completedWork):
            if start_byte>=item[1] and start_byte<=(item[1]+item[2]-1):
                sIndex= i
                break;
        #print 'indexfound',sIndex
        if sIndex==-1: return False;#not downloaded yet
        return True

    def get_downloaded_len_from(self, start_byte, stopAt): #tell us how many bytes are downloaded
        try:
            sIndex=-1;
            #print 'check downloaded',start_byte,stopAt
            #print 'self.completedWork',self.completedWork
            t_comwork=sorted(self.completedWork,key=lambda x: x[1])
            for i, item in enumerate(t_comwork):
                if start_byte>=item[1] and start_byte<=(item[1]+item[2]-1):
                    sIndex= i
                    break;
            #print 'indexfound',sIndex
            if sIndex==-1: return 0;#not downloaded yet
            eIndex= t_comwork[sIndex][1]+t_comwork[sIndex][2]-1;
            #print 'eIndex',eIndex
            for i in range(sIndex+1, len(t_comwork)) :  #forward till we find a gap or it ends
                if t_comwork[i-1][1]+t_comwork[i-1][2]==t_comwork[i][1]: #if new chunk is joint with previous one
                    eIndex=t_comwork[i][1]+t_comwork[i][2]-1;#add new length
                    #print 'eIndexInside',eIndex,stopAt
                else:
                    break;
                if eIndex>stopAt:
                    eIndex=stopAt;
                    break;
            if eIndex>stopAt:
                eIndex=stopAt;
       
            return eIndex-start_byte+1;
        except Exception,e:
            print 'Failed in stop_downloader  #%s :'%e 
            return 0

    def get_downloaded_data_from(self, start_byte,end_byte): #read the downloaded file,return whatever is downloaded so far starting from start_byte, could return less

        
        downloadBytes=self.get_downloaded_len_from(start_byte,end_byte);
        #print 'downloadBytes:',downloadBytes
        if downloadBytes==0: return "",0;

        self.saveFileLock.acquire();
        out_fd = open(self.fileFullPath, "rb")
        positionToRead=start_byte
        filesizeToRead= downloadBytes;
        dataToReturn=""
        out_fd.seek(positionToRead)
        #print 'seek',positionToRead,'read',filesizeToRead
        dataToReturn=out_fd.read(filesizeToRead)
        out_fd.close();
        self.saveFileLock.release()
        #print dataToReturn
        return dataToReturn,filesizeToRead
        #read from file, from sIndex to eIndex
 
    def freeze_all_threads(self, freeze):
        for t,c in self.currentThreads:
            if freeze:
                c.acquire()
            else:
                c.release()
    def repriotize_queue(self,  startingByte):# shuffle the queue and start downloading what xbmc wants, due to seek may be?
        print 'stop everyone, repriotize_queue',startingByte
        self.stopEveryone=True
        print 'wait for the condition'

        #self.isAllowed.acquire();#freeze everyone
        #time.sleep(2); #give time so everyone are frozen
        self.freeze_all_threads(True)
        print 'thread frozen'
        self.saveFileLock.acquire(); #try again
        self.isAllowed.acquire();
        print 'save frozen'
        downloadBytes=self.get_downloaded_len_from(startingByte,startingByte+10); #just check again
        
        if not downloadBytes==0: 
             self.isAllowed.release()
             self.freeze_all_threads(False)
             self.saveFileLock.release();
             return #transient situation
        print 'ok start looking into'
        currentQueue=[];
        while (not self.workQ.empty()):  #clear the queue
            currentQueue.append(self.workQ.get())
        print 'left over',currentQueue
        currentQueue.sort(key=lambda x: x[2]);# sort on start number as we could be in any sequence due to seek,,front to back and so on
        sIndex=-1
        for i, item in enumerate(currentQueue):
            if startingByte>=item[2] and startingByte<=(item[2]+item[3]-1):
                sIndex= i
                break;
        print 'sIndex starting point',sIndex
        if not sIndex==-1: #error here !
            newQueue=[]
            for i in range(0,len(currentQueue)):
                currentQueue[sIndex][0]=i; #new priority
                newQueue.append(currentQueue[sIndex])
                sIndex+=1;
                if sIndex>len(currentQueue)-1: sIndex=0;#if reached end then start from beginning
            print 'newQueue',newQueue
            for i, item in enumerate(newQueue):
                self.workQ.put(item)# recreate new queue in different order
        self.stopEveryone=False
        self.freeze_all_threads(False)
        self.saveFileLock.release();
        self.isAllowed.release(); #start downloading again but in different priority
        

    #def stop()
    #    StopFreeAllrunningthreads;


    def __get_file_size(self, url):
        '''
        Gets file size in bytes from server
        
        Args:
            url (str): full url of file to download
        '''  
        
        request = urllib2.Request(url, None, self.http_headers)
        
        try:
            data = urllib2.urlopen(request)
            content_length = data.info()['Content-Length']
        except urllib2.URLError, e:
            #common.addon.log_error('http connection error attempting to retreive file size: %s' % str(e))
            print 'http connection error attempting to retreive file size: %s' % str(e)
            return False
  
        return content_length
    
    
    def __save_file(self, out_file):
        '''
        Processes items in resultQ and saves each queue/chunk to disk

        Args:
            file_dest (str): full path to save location - EXCLUDING file_name
            file_name (str): name of saved file
        '''

        while True:
            try:
                if self.stopProcessing: return
                self.saveFileLock.acquire()
                #Grab items from queue to process
                try:
                    block_num=-1
                    block_num, start_block,length, chunk_block = self.resultQ.get(block=False,timeout=1)  
                except Exception, e:
                    self.saveFileLock.release()
                    continue
                
                #print 'trying to get the first chunk'

                #Write downloaded blocks to file
                common.addon.log('Writing block #%d starting byte: %d size: %d' % (block_num, start_block, len(chunk_block)), 2)
                
                out_fd = open(out_file, "r+b")      
                out_fd.seek(start_block, 0)
                out_fd.write(chunk_block)
                out_fd.close()
                
                #Tell queue that this task is done
                self.resultQ.task_done()
                self.completedWork.append ([block_num, start_block,length])


            except Exception, e:
              
                common.addon.log_error('Failed writing block #%d :'  % (block_num, e))        
                
                #Put chunk back into queue, mark this one done
                self.resultQ.task_done()
                self.resultQ.put([block_num, start_block,length, chunk_block])    
            
            self.saveFileLock.release()

    def __build_workq(self, file_link):
        '''
        Determine file size
        
        Build work queue items based on chunk_size

        Args:
            file_link (str): direct link to file including file name
            
        '''
        
        #Retreive file size
        remaining = int(self.__get_file_size(file_link))
        self.fileLen= remaining
        common.addon.log('Retrieved File Size: %d' % remaining, 2) 
             
        # Split file size into chunks
        # Add each chunk to a queue spot to be downloaded individually
        # Using counter i to determine chunk # / priority
        start_block = 0
        chunk_block = self.chunk_size
        i = 0
        
        while chunk_block > 0:
 
            #Add chunk to work queue 
            #print 'adding chunk',[i, file_link, start_block, chunk_block]
            self.workQ.put([i, file_link, start_block, chunk_block])
        
            #Increment starting byte
            start_block += chunk_block
            
            #Reduce remaining bytes by size of chunk
            if remaining >= chunk_block:
                remaining -= chunk_block
        
            #If remaining is less than size of chunk, we want the final chunk to be what's left
            if remaining < chunk_block:
                chunk_block = remaining
        
            #Increment i - used to set queue priority
            i += 1
        self.total_chunks=i
    

    def download(self, file_link, file_dest='', file_name='',start_byte=0):
        '''
        Main function to perform download
              
        Args:
            file_link (str): direct link to file including file name
        Kwargs:
            file_dest (str): full path to save location - EXCLUDING file_name        
            file_name (str): name of saved file - name will be pulled from file_link if not supplied
        ''' 

        common.addon.log('In Download ...', 2)
        if not file_dest:
            file_dest = common.profile_path
               
        # Create output file with a .part extension to indicate partial download
        if not os.path.exists(file_dest):
            os.makedirs(file_dest)
            
        out_file = os.path.join(file_dest, file_name)
        part_file = out_file + ".part"
        out_fd = os.open(out_file, os.O_CREAT | os.O_WRONLY)
        os.close(out_fd)
        self.fileFullPath=out_file
        self.filename = file_name
        
        common.addon.log('Worker threads processing', 2)

        self.isAllowed.acquire();
        self.__build_workq(file_link) 
        
        # Ccreate a worker thread pool
        for i in range(self.num_conn):
            keepProcessing = multiprocessing.Condition()
            t = DownloadQueueProcessor()
            t.setKeepProcessing(keepProcessing)
            t.caller = self
            self.currentThreads.append([t,keepProcessing])
            t.start()
        common.addon.log('Worker threads initialized', 2)
        
        # Save downloaded chunks to file as they enter the resultQ
        # Put process into it's own thread
        st = threading.Thread(target=self.__save_file, args = (out_file, ))
        st.start()

        common.addon.log('Result thread initialized')            
        
        #Build workQ items

        self.isAllowed.release()
        common.addon.log('Worker Queue Built', 2) 
        self.started=True  
        # Wait for the queues to finish - join to close all threads when done
        while True:
            if self.stopProcessing:
                break;
            self.isAllowed.acquire()
            #print 'isallowed acquire..'
            remaining=self.total_chunks - len(self.completed_work())
            #print 'remaing tasks',remaining
            self.isAllowed.release()

            if remaining>0:
                time.sleep(5);
            else:
                break;
        
        if not self.stopProcessing:
            print 'now final join'
            #self.workQ.join()#timeout# This freezes, since we are here and everything has been processed ..
            print 'resultQ', self.resultQ.unfinished_tasks
            common.addon.log('Worker Queue successfully joined', 2)
            if self.resultQ.unfinished_tasks:
                print 'there are still some unfinsihed tasks??'
                time.sleep(6)#give time to results to finish.
            if not self.resultQ.unfinished_tasks:
                self.resultQ.join()
                common.addon.log('Result Queue successfully joined', 2)
            else:
                print 'something wrong, tasks are finished but results are not in,ignoring'
            self.completed=True
            print 'terminating'
            self.terminate()
            print 'DOWNLOAD COMPLETED'
        else:
            print 'Ternimated... by user'
        print 'THE END'
        self.terminated=True
        #if self.download_mode==1 and not self.keep_file: # if file not to be saved
        #    os.remove(out_file)
        #todo: delete if saving not required.
        #Rename file from .part to intended name
        #os.rename(part_file, out_file)
    
    def cleanup(self, fromStreamer=False):
        if (not fromStreamer) and self.download_mode==1:
            return # if its not streamer and file is in streaming mode then it may be in use
        if (not self.completed) or (self.download_mode==1 and not self.keep_file): # if not finished downloading or its streaming but saving not req.
            os.remove(self.fileFullPath)
        


class DownloadQueueProcessor(threading.Thread):
    def __init__(self):
        '''
        Class init      
        
        Inherits threading.Thread
        '''    	
        threading.Thread.__init__(self)

        self.http_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; '
                'en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            'Accept': 'text/xml,application/xml,application/xhtml+xml,'
                'text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
            'Accept-Language': 'en-us,en;q=0.5',
        }
        self.block_size = 1024*500
        self.stopProcessing=False
        self.keepProcessing=None
        self.caller=None

    def terminate(self):
        self.stopProcessing=True

    def setKeepProcessing(self,C):
        self.keepProcessing=C


    def run(self):
        '''
        Override threads run() method to do our download work
        '''

        while True:
            if self.stopProcessing: return
            self.keepProcessing.acquire()
            #self.caller.isAllowed.acquire();
            try:
                block_num=-1
                block_num, url, start, length = self.caller.workQ.get(block=False,timeout=1) ##put a time out here
            except Exception, e:
                self.keepProcessing.release()
                pass
            #self.caller.isAllowed.release();
            
            if not self.caller.workQ.unfinished_tasks:
                #self.keepProcessing.release()
                print 'end of thread................'
                return
            #common.addon.log('Starting Worker Queue #: %d starting: %d length: %d' % (block_num, start, length), 2)

            if block_num==-1:
                time.sleep(1)
                continue 
            #Download the file
            start_time = time.time()
            result,chunkData = self.__download_file(block_num, url, start, length)
            elapsed_time = time.time() - start_time
            #print 'time take ',elapsed_time
            #Check result status            
            if result == True:
                #Tell queue that this task is done
                #common.addon.log('Worker Queue #: %d downloading finished' % block_num, 2)
                
                #Mark queue task as done
                
                
                #common.addon.log('Adding to result Queue #: %d' % block_num, 2)
                self.caller.resultQ.put([block_num, start,length, chunkData])
                self.caller.workQ.task_done()
                
                #isAllowed.acquire();
                
                #print [block_num, start,length]
                #isAllowed.release();

            #503 - Likely too many connection attempts
            elif result == "503":

                common.addon.log('503 error - Breaking from loop, closing thread - Queue #: %d' % block_num, 0)
                
                #isAllowed.acquire();
                #Mark queue task as done
                self.caller.workQ.task_done()
                
                #Put chunk back into workQ then break from loop/end thread
                self.caller.workQ.put([block_num, url, start, length])
                #isAllowed.release();
                break

            else:
                #Mark queue task as done
                #isAllowed.acquire();
                self.caller.workQ.task_done()
            
                #Put chunk back into workQ
                common.addon.log('Re-adding block back into Queue - Queue #: %d' % block_num, 0)
                self.caller.workQ.put([block_num, url, start, length])
                #isAllowed.release();
            self.keepProcessing.release()

 
    def __download_file(self, block_num, url, start, length):        
        '''
        download worker function
              
        Args:
            block_num (int): where in the file this block belongs
            url (str): direct link to file for download
            start (int): starting block to download from
            length (int): length of bytes to read for this block
        ''' 
        request = urllib2.Request(url, None, self.http_headers)
        if length == 0:
            return None,""
        request.add_header('Range', 'bytes=%d-%d' % (start, start + length))

        if self.caller.stopEveryone: return None,"";
        #TO-DO: Add more url type error checks
        while 1:
            try:
                data = urllib2.urlopen(request)
            except urllib2.URLError, e:
                common.addon.log_error("Connection failed: %s" % e)
                return str(e.code),""               
            else:
                break

        if self.caller.stopEveryone: return None,"";
        #Init working variables 
        #print 'testing here'
        curr_chunk = ''
        remaining_blocks = length
        dataLen=0
        #Read data blocks in specific size 1 at a time until we have the full chunk_block size
        while remaining_blocks > 0:
            #print 'remaining_blocks',remaining_blocks
            if self.caller.stopEveryone: return None,"";

            if remaining_blocks >= self.block_size:
                fetch_size = self.block_size
            else:
                fetch_size = int(remaining_blocks)
            #print 'fetch_size',fetch_size
            try:
                data_block = data.read(fetch_size)
                dataLen=len(data_block)
                #print 'got data' ,dataLen
                if dataLen == 0:
                    print 'zeroooooooooooooooooooooooooo'
                    common.addon.log("Connection: 0 sized block fetched. Retrying.", 0)
                    return "no_block",""
                #if len(data_block) != fetch_size:
                #    print 'mismatche.............................'
                #    common.addon.log("Connection: len(data_block) != length. Retrying.", 0)
                #    return "mismatch_block",""

            except socket.timeout, s:
                common.addon.log_error("Connection timed out with msg: %s" % s)
                return "timeout",""
            except Exception, e:
                common.addon.log_error("Error occured retreiving data: %s" % e)
                return "data_error",""

            #remaining_blocks -= fetch_size
            remaining_blocks-= dataLen
            curr_chunk += data_block
            #print 'next chunk size',len(curr_chunk), remaining_blocks
        #print 'done one chunk'
        #print 'current completed',completedWork
        return True,curr_chunk