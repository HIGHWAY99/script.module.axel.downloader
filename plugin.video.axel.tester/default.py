import xbmc, xbmcgui, xbmcaddon, xbmcplugin
import urllib, urllib2
import re, string
import threading
import os
import base64
from t0mm0.common.addon import Addon
from t0mm0.common.net import Net
import axel

addon = Addon('plugin.video.axel.tester', sys.argv)
net = Net()

mode = addon.queries['mode']
play = addon.queries.get('play', None)
url = addon.queries.get('url', None)
name = addon.queries.get('name', None)
imdb_id = addon.queries.get('imdb_id', None)
tmdb_id = addon.queries.get('tmdb_id', None)
video_type = addon.queries.get('video_type', None)
season = addon.queries.get('season', None)

    
if mode == 'main':
          
    file_link ='http://lwx003.gear3rd.net/files/videos/2014/03/25/1395763892aba24-240.mp4'
    #file_link='http://download.wavetlan.com/SVV/Media/HTTP/H264/Other_Media/H264_test5_voice_mp4_480x360.mp4'
    
    liz=xbmcgui.ListItem('Video')
    liz.setInfo( type="Video", infoLabels={ "Title": 'Video' } )
    u = sys.argv[0] + "?url=" + file_link + "&mode=play1"
    ok=xbmcplugin.addDirectoryItem(handle=int(sys.argv[1]),url=u,listitem=liz,isFolder=False)

    liz2=xbmcgui.ListItem('Video 2')
    liz2.setInfo( type="Video", infoLabels={ "Title": 'Video 2' } )
    u = sys.argv[0] + "?url=" + file_link + "&mode=play2"  
    ok=xbmcplugin.addDirectoryItem(handle=int(sys.argv[1]),url=u,listitem=liz2,isFolder=False)
   
    
elif mode == "play1":
   
    axel = axel.AxelDownloader()  
    file_dest = addon.get_profile()
    file_name = url.split('/')[-1]
        
    liz=xbmcgui.ListItem('Video')
    liz.setInfo( type="Video", infoLabels={ "Title": 'Video' } )
    
    #dt = threading.Thread(target=axel.download, args = (url, file_dest, file_name))
    #dt.start()
      
    #if dt.isAlive():
    #    full_path = os.path.join(file_dest, file_name)
    #    addon.show_countdown(30, 'waiting', 'waiting for first part')      
    xbmc.Player(xbmc.PLAYER_CORE_AUTO).play(url)

elif mode == "play2":
   
    axel = axel.AxelDownloader()  
    file_dest = addon.get_profile()
    file_name = url.split('/')[-1]
        
    liz=xbmcgui.ListItem('Video 2')
    liz.setInfo( type="Video", infoLabels={ "Title": 'Video 2' } )
    	
    link = 'http://127.0.0.1:%s/'%'45550' + base64.b64encode(url)
    xbmc.Player(xbmc.PLAYER_CORE_AUTO).play(link)


if not play:
    addon.end_of_directory()