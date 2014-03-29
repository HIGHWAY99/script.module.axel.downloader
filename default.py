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


import common
import proxy
# This is xbmc linked class. TODO: read the settings here and send it to proxy for port etc
#TODO: check if start at launch setting is configured!



if __name__ == '__main__':  
    file_dest = common.profile_path #todo: get everything we need to read from settings of xbmc
    proxy.ProxyManager().start_proxy(download_folder=file_dest), #more param to come

