#!/usr/bin/env python
import re
import urllib2
import os
from datetime import datetime
from datetime import timedelta
import time
import pytz
import subprocess
import collections
import calendar
import sys
import argparse
import socket
import radix # http://code.google.com/p/py-radix/
import ipaddr # http://code.google.com/p/ipaddr-py/
import bz2
import traceback
import math
#import cProfile
##from guppy import hpy

#### output /ncc/sgscratch/ris_cc_test

## GLOBAL config
PYRIS_RAW_DIR=os.environ['HOME'] + '/data/raw/RIS_CC'
PYRIS_CMD_BGPDUMP='/home/inrdb/svn/sg/projects/INRDB/libbgpdump/bgpdump'
PYRIS_RRCS=range(0,16+1) ### what RRCs to try (numeric values)
PYRIS_OUTPUT_DIR='/ncc/sgscratch/ris_cc_test'
### min power to be defined routed: https://labs.ripe.net/Members/wilhelm/content-how-define-address-space-routed?searchterm=define+routed
PYRIS_MIN_POWER=10
PYRIS_INRDB_HOST='127.0.0.1'
PYRIS_MIN_BVIEW_TOLERANCE=8*3600; # 8 HRS tolerance for bviews being earlier then start_ts (expressed in seconds!)
PYRIS_DELAY_AFTER_MAJORITY=10*60; # wait 10 mins after getting data for a majority of RRCs for other RRCs to catch up
PYRIS_UPDATE_FILE_LENGTH=300; ## what period does an update file cover. 5mins
### one can override PYRIS vars from local.py
try:
   from local import *
except:
   pass
### TODO: override from environment?

### PFX holds the global table per peer
PFX={}
### DOWN_PEERS
##   holds any peer whose last bgp status update was not status_no 6
DOWN_PEERS={}

### global struct that holds FILE locations (remote and local) and open file-descriptors to these
### TODO: reimplement RIS_FILES dict as a proper class
class RisFiles:
   def __init__(self):
      self.rf = {}

class RRCFileQueue: ## set up a queue for an RRC
   def __init__(self, rrc): 
      self.rrc = rrc
      self.queue = []
      self.queue_sorted = False
      self.min_bview_ts = None ### timestamp of the earliest bview that we'll need to process
      self.last_web_sync_ts = None ### timestamp of when sync with webdir was performed last (ie. check for files on RIS data URL)
      self.last_opened_file_ts = None ### timestamp of the last file that has been opened for processing
      self.process_ts = 0 ### timestamp of processing queue (for bview: ts on file, for update: ts in the bgpdump line)
      self.pipe = None ### filedescriptor to libbgpdump
      self.buf = None ### contains processed line that was read from filedesc, but is not passed on yet
   def fetch_from_webdir(self,start_ts,end_ts,fresh=True):
      start_dt = datetime.utcfromtimestamp( start_ts )
      end_dt = datetime.utcfromtimestamp( end_ts )
      iter_dt = start_dt.replace(day=1)
      while iter_dt <= end_dt:
         self.__fetch_from_webdir_yyyy_mm(start_ts, iter_dt)
         if ( iter_dt.month >= 12 ):
            iter_dt = iter_dt.replace(month=1, year=iter_dt.year + 1 )
         else:
            iter_dt = iter_dt.replace(month=iter_dt.month + 1 )
      if not self.min_bview_ts: ### we didn't find a bview on/right before start_dt, we need to scan previous month too
         if ( start_dt.month <= 1):
            iter_dt = start_dt.replace(day=1, month=12, year=start_dt.year - 1)
         else:
            iter_dt = start_dt.replace(day=1, month=start_dt.month - 1)
         self.__fetch_from_webdir_yyyy_mm(start_ts, iter_dt)
      ## now prune everything before min_bview
      if fresh: ### fresh: we need to go back to a time where we have a bview (to build up the initial table)
         if self.min_bview_ts:
            self.queue = [file for file in self.queue if file['ts'] >= self.min_bview_ts and file['ts'] <= end_ts]
         else: # no min_bview means: maybe there are updates only since start_ts ?
            self.queue = [file for file in self.queue if file['ts'] >= start_ts and file['ts'] <= end_ts]
      else: ### just prune to: between start (non-inclusive!) and end ts
         self.queue = [file for file in self.queue if file['ts'] > start_ts and file['ts'] <= end_ts]
      if len( self.queue ):
         self.__sortqueue()
      ##print "fetch_from_webdir: %s" % ( self.queue )
      print "fetch_from_webdir (rrc%02d): queue: %d files now" % ( self.rrc, len( self.queue ) )
   def read_upto(self, ts, callback ):
      '''read lines from bgpdump upto (and including) the specified timestamp and apply function callback to it, returns the number of lines read'''
      cnt=0
      ### if there is something in 'buf'. do callback on that first
      if self.buf and ts >= self.process_ts:
         callback( self.buf )
      while ts >= self.process_ts:
         if len( self.queue ) == 0: # nothing to process at this point (note we leave the file that is processed currently on the queue)
            return
         if not self.pipe:
            self.__open_next_file()
         try:
            bgpline = [self.rrc]
            b = self.pipe.next()
            b = b.rstrip('\n')
            buf = b.split('|')
            if self.__bgp_line_ok( buf ):
               buf[1] = int(buf[1]) ## timestamp is numeric
            else:
               continue
            if self.queue[0]['type'] == 'updates':
               self.process_ts = buf[1]
            ### ts on opening bviews is set when opening the file and determined by the ts on the filename
            bgpline = bgpline + buf
            if ts >= self.process_ts:
               callback(bgpline) 
               cnt+=1
            else:
               self.buf = bgpline
               break
         except StopIteration:
            try:
               self.pipe.close()
            except:
               pass
            self.pipe = None
            self.__remove_file_from_queue()
         except:
            print "other exception in read_upto %s %s" % ( self.rrc, self.queue )
      print "read_upto: callback executed %g times" % ( cnt )
      return cnt
#### RRCFileQueue internal funcs
   def __bgp_line_ok( self, buf ):
      '''do some basic checks on validity of bgpdump output'''
      ## maybe make this better?
      if len( buf ) >= 6: 
         return True
      else:
         print "libbgpdump weird output (rrc%02d), file (%s): %s" % (self.rrc, self.queue[0], buf)
         return False 
   def __remove_file_from_queue( self ):
      try:
         os.remove( self.queue[0]['local_file'] )
      except:
         print "rrc%02d couldn't remove from %s" % ( self.rrc, self.queue )
      
      if len( self.queue ) > 0:
         self.queue = self.queue[1:]
   def __open_next_file(self):
      next_file = self.queue[0]
      print "opening next file %s " % ( next_file )
      if not 'local_file' in next_file:
         self.__fetch_next_file()
      if next_file['type'] == 'bview':
         cmd = "%s -m -v -t change %s" % ( PYRIS_CMD_BGPDUMP, next_file['local_file'] )
      elif next_file['type'] == 'updates':
         cmd = "%s -m -v %s" % ( PYRIS_CMD_BGPDUMP, next_file['local_file'] )
      else:
         raise RuntimeError("Unknown bgpdump file type: '%'" % ( next_file['type'] ))
      self.pipe = subprocess.Popen(cmd, shell=True, bufsize=1024*8, stdout=subprocess.PIPE).stdout
      self.last_opened_file_ts = next_file['ts']
      if next_file['type'] == 'bview':
         self.process_ts = next_file['ts']
   def __fetch_next_file(self):
      try:
         next_file = self.queue[0]
         local_dir = "%s/rrc%02d" % (PYRIS_RAW_DIR, self.rrc)
         local_dir_cmd = "mkdir -p %s" % ( local_dir )
         os.system( local_dir_cmd )
         local_file = '/'.join([ local_dir, next_file['file'] ])
         fetch_cmd="wget -q -O %s %s/%s" % ( local_file, next_file['remote_dir'], next_file['file'] )
         print fetch_cmd
         os.system( fetch_cmd )
         self.queue[0]['local_file'] = local_file
      except:
         print "something went wrong fetching %s" % ( self.queue[0] )
   def __sortqueue(self):
      self.queue.sort(key=lambda file: file['type']) ## 'bviews' before 'updates'
      self.queue.sort(key=lambda file: file['ts']) ## sort on datetime
      self.queue_sorted = True
   def __fetch_from_webdir_yyyy_mm(self, start_ts, start_of_month_dt):
      self.last_web_sync_ts = int( time.time() ) # record when last timesync attempt was
      rrc_url = "http://data.ris.ripe.net/rrc%02d/%04d.%02d/" % ( self.rrc, start_of_month_dt.year, start_of_month_dt.month )
      try:
         response = urllib2.urlopen( rrc_url )
      except:
         print "fetch_from_webdir: error trying to fetch %s" % ( rrc_url )
         return
      msg=response.read()
      for risfile in linkregex.findall(msg):
         (filetype, file_ts) = info_from_ris_filename( risfile )
         if filetype == 'bview':
            ### only consider this RRC if first bview is less then TOLERANCE (8hrs) before start_ts (or right on start_ts)
            if file_ts <= start_ts and file_ts > start_ts - PYRIS_MIN_BVIEW_TOLERANCE:
               if   self.min_bview_ts == None:    self.min_bview_ts = file_ts
               elif self.min_bview_ts > file_ts:  self.min_bview_ts = file_ts
         elif filetype == 'updates':
            pass
         else:
            continue
         self.queue.append({
            'file': risfile,
            'remote_dir': rrc_url,
            'type': filetype,
            'ts': file_ts
         })


RIS_FILES={}

### ACTIVE_PEERS
###  holds per RRC set of peers that have been seen as providing BGP updates (not STATE msgs only)
###  ie. ACTIVE_PEERS[rrc][peer_ip]
class ActivePeers:
   def __init__(self):
      self.active_peers = {}
   def update( self, rrc, peer_ip ):
      if rrc in self.active_peers:
         self.active_peers[rrc].add( peer_ip )
      else:
         self.active_peers[rrc] = set([ peer_ip ])
   def is_rrc_active( self, rrc):
      if rrc in self.active_peers: return True
      else:                        return False
   def reset_rrc( self, rrc ):
      if rrc in self.active_peers:
         del( self.active_peers[rrc] )      

ACTIVE_PEERS=ActivePeers()

### CcRtree holds v4+v6 pfx->cc mappings in an efficient patricia tree
## also ASN->cc mappings in a simple dictionary
class CcRtree:
   def __init__( self ):
      self.radix = radix.Radix()
      self.asns = {}
      self.cc = {}
   def getCcStats( self, cc ):
      v4 = 0
      v6 = 0
      asn = 0
      if cc in self.cc:
         if 'v4' in self.cc[cc]:
            v4=self.cc[cc]['v4']
         if 'v6' in self.cc[cc]:
            v6=self.cc[cc]['v6']
         if 'asn' in self.cc[cc]:
            asn=self.cc[cc]['asn']
      return (v4,v6,asn)
   def checkIfFilled( self, cclist ):
      ''' 
      checks if list is filled, using specified list of countrycodes 
      returns True if all of the countrycode buckets are filled,
      returns False otherwise
      '''
      for cc in cclist:
         (v4,v6,asn) = self.getCcStats( cc )
         if v4 == 0 or v6 == 0 or asn == 0:
            return False
      return True
             
   def getCcFromPfx( self, pfx ):
      rnode = self.radix.search_best( pfx )
      if rnode:
         return rnode.data['cc']
      else:
         return None
   def getCcFromAsn( self, asn ):
      if asn in self.asns:
         return self.asns[asn]
      else:
         return None
   def addIp( self, pfx, cc, recursive=False ):
      if not recursive:
         rtype = 'v4'
         if pfx.count(':'):
            rtype = 'v6'
         if cc in self.cc:
            if rtype in self.cc[cc]:
               self.cc[cc][rtype] += 1
            else:
               self.cc[cc][rtype] = 1
         else:
            self.cc[cc] = {rtype: 1}
      r = self.radix.add( pfx )
      r.data['cc'] = cc
      # find other half of the first covering prefix 
      other_half = None
      if r.family == socket.AF_INET:
         this_half = ipaddr.IPv4Address( r.network )
         other_half = ipaddr.IPv4Address( int(this_half) ^ 2 ** ( 32 - r.prefixlen ) )
      elif r.family == socket.AF_INET6:      
         this_half = ipaddr.IPv6Address( r.network )
         other_half = ipaddr.IPv6Address( int(this_half) ^ 2 ** ( 128 - r.prefixlen ) )
      else:
         raise RuntimeException('unknown ip address family: %s' % ( rnode.family ) )
      #print "other half %s %s %s" % ( r.network, other_half, r.prefixlen )
      if self.radix.search_exact( str(other_half), r.prefixlen ):
         r_oh = self.radix.search_exact( str(other_half), r.prefixlen )
         if r_oh.data['cc'] == r.data['cc']:
            # delete the 2 halves and add the supernet 
            sup_pfxlen = r.prefixlen - 1
            sup_addr = None
            if r.family == socket.AF_INET:
               sup_addr = ipaddr.IPv4Address( int(other_half) & ~2 ** ( 32 - r.prefixlen ) )
            elif r.family == socket.AF_INET6:
               sup_addr = ipaddr.IPv6Address( int(other_half) & ~2 ** ( 128 - r.prefixlen ) )
            else:
               raise RuntimeException('unknown ip address family: %s' % ( rnode.family ) )
            #print "merging %s %s (%s) => %s (%s) " % ( r.network, r_oh.network, r.prefixlen, sup_addr, sup_pfxlen )
            r_sup = self.addIp(
               '/'.join([str(sup_addr), str(sup_pfxlen)]),
               cc,
               recursive=True
            )
            self.radix.delete( r.network, r.prefixlen )
            self.radix.delete( r_oh.network, r_oh.prefixlen )
      return r
   def addAsn( self, asn, cc):
      asn = str(asn) # make sure its a string
      self.asns[asn] = cc
      if cc in self.cc:
         if 'asn' in self.cc[cc]:
            self.cc[cc]['asn'] += 1
         else:
            self.cc[cc]['asn'] = 1
      else:
         self.cc[cc] = {'asn': 1}
## end CcRtree

## InrdbSocket: communicate with local INRDB instance
class InrdbSocket:
   def __init__( self, sock=None):
      if sock is None:
            self.sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            self.sock.connect((PYRIS_INRDB_HOST, 5555))
            self.recvlines = []
            self.recvbufpart = ''
   def mysend(self, msg):
      ## convenience: make sure msg has \n at the end
      msg = msg.rstrip('\n') + '\n'
      totalsent = 0
      while totalsent < len(msg):
         sent = self.sock.send( msg[totalsent:])
         if sent == 0:
            raise RuntimeError("socket connection broken")
         totalsent = totalsent + sent
   def myreceive(self):
      #return self.fd.readline()
      while len( self.recvlines ) == 0:
         chunk = self.sock.recv(1024)
         if chunk == '':
            raise IOError("end of file")
         chunk = self.recvbufpart + chunk
         if chunk.rsplit('\n'):
            chunk += '\n'
         self.recvlines = chunk.splitlines()
         self.recvbufpart = self.recvlines.pop()
      ret_line = self.recvlines[0]
      self.recvlines = self.recvlines[1:]
      return ret_line
   def myclose(self):
       self.sock.close()
## end InrdbSocket

#### END CLASSES

#### BEGIN REGEX

linkregex = re.compile('<a\s*href=[\'|"](.*?)[\'"].*?>')
risfileregex = re.compile('(bview|updates)\.(\d{4})(\d{2})(\d{2})\.(\d{2})(\d{2})\.gz')
inrdbline_re=re.compile('(BLOB|RES):\s+(.*)')
single_asn_re=re.compile('AS(\d+)$')
range_asn_re=re.compile('AS(\d+)\-AS(\d+)$')

#### END REGEX

#### BEGIN FUNCTIONS

## stats reading
def read_cc_stats_from_inrdb( ccrtree, ts ):
   for r_type in ['0/0','::/0']:
      s = InrdbSocket()
      msg = '+dc RIR_STATS -m %s +xt %d +T +d +M +R\n' % ( r_type, ts )
      s.mysend(msg)
      cc = ''
      while True:
         try:
            line=s.myreceive()
            match = re.match( inrdbline_re, line )
            if match:
               if match.group(1) == 'BLOB':
                    cc = match.group(2).split('|')[1]
               elif match.group(1) == 'RES':
                     res = match.group(2)
                     ccrtree.addIp(res,cc)
         except IOError:
            break
      s.myclose()
   ## asns
   s = InrdbSocket()
   msg = '+dc RIR_STATS -m AS* +xt %d +T +d +M +R\n' % ( ts )
   s.mysend(msg)
   cc = ''
   while True:
      try:
         line=s.myreceive()
         match = re.match( inrdbline_re, line )
         if match:
            if match.group(1) == 'BLOB':
               cc = match.group(2).split('|')[1]
            elif match.group(1) == 'RES':
               res_str = match.group(2)
               s_match = re.match( single_asn_re, res_str)
               if s_match:
                  asn = s_match.group(1)
                  ccrtree.addAsn(asn,cc)
               else:
                  r_match = re.match( range_asn_re, res_str)
                  if r_match:
                     start_asn = int(r_match.group(1))
                     end_asn = int(r_match.group(2))
                     for asn in range(start_asn,end_asn+1):
                        ccrtree.addAsn(asn,cc)
      except IOError:
         break
   s.myclose()

def find_ris_files( start_dt, end_dt ):
   ris_files={}
### need to remove these and go to full unix_ts only mode
   start_ts = calendar.timegm( start_dt.timetuple() )
   end_ts = calendar.timegm( end_dt.timetuple() )

   for rrc_no in PYRIS_RRCS:
      iter_dt = start_dt.replace(day=1)
      rrc_info = {
         'min_bview': None,  ### holds the bview file where processing should start
         'files': []
      }
      timesync = None
      while iter_dt <= end_dt:
        timesync = find_ris_files_rrc_yymm( rrc_info, rrc_no, iter_dt, start_dt )
        if ( iter_dt.month >= 12 ):  
                iter_dt = iter_dt.replace(month=1, year=iter_dt.year + 1)
        else:
                iter_dt = iter_dt.replace(month=iter_dt.month + 1)
      ## record the timesync of the last dir fetch (ie. closest to end_dt)
      rrc_info['last_rrc_dir_sync_ts'] = timesync
      
      ### if we didn't find a bview that starts before start_dt (min_bview) we need to scan
      ## previous month too :(
      # see http://data.ris.ripe.net/rrc11/2011.09/
      if not rrc_info['min_bview']:
         if ( start_dt.month <= 1):
            iter_dt = start_dt.replace(month=12, year=start_dt.year - 1)
         else:
            iter_dt = start_dt.replace(month=start_dt.month - 1)
         find_ris_files_rrc_yymm( rrc_info, rrc_no, iter_dt, start_dt )
      ### if still no min_bview , don't put this rrc onto the ris_files
      if not rrc_info['min_bview']:
         continue
      ## now prune and sort this list of files
      #print "pre-pruning lenght %d" % ( len( rrc_info['files'] ) )
      files = [file for file in rrc_info['files'] if file['ts'] >= rrc_info['min_bview'] and file['ts'] <= end_ts]
      #print "post-pruning length %d (rrc %d) " % ( len(files), rrc_no )
      if len(files):
         files.sort(key=lambda file: file['type']) ## 'bviews' before 'updates'
         files.sort(key=lambda file: file['ts']) ## sort on datetime
         rrc_info['files'] = files
         ris_files[rrc_no] = rrc_info
         print "rrc%02d : last file ts: %d, latest sync ts: %d, diff: %d" % ( rrc_no, files[-1]['ts'] , rrc_info['last_rrc_dir_sync_ts'] , rrc_info['last_rrc_dir_sync_ts'] - files[-1]['ts'] )
         #ris_file
         #   'files': files,
         #}
         #print "found %d files for rrc%02d" % ( len(files), rrc_no )
         #print "1st file to process %s (type %s)" % (files[0]['file'], files[0]['type'] )
         #print "2nd file to process %s (type %s)" % (files[1]['file'], files[1]['type'] )
   return ris_files

def info_from_ris_filename( risfile ):
   match = re.match(risfileregex, risfile)
   if match:
      filetype = match.group(1)
      year = int(match.group(2))
      month = int(match.group(3))
      mday = int(match.group(4))
      hour = int(match.group(5))
      mins = int(match.group(6))
      file_ts = calendar.timegm( (year,month,mday,hour,mins,0))
      return (filetype, file_ts)
   else:
      return (None, None)

#########
### functions that update the table
#########
def update_table( buf ):
#  0   1              2             3    4                5       6             7
# [11, 'TABLE_DUMP2', '1314835199', 'B', '198.32.160.61', '6939', '1.0.4.0/22', '6939 7545 7545 7545 7545 7545 56203', 'IGP', '198.32.160.61', '0', '783', '', 'NAG', '', '\n']
   #if buf[6] and buf[6] == '110.196.16.0/22':
   #   print "DEBUG: %s" % (buf)
   peer_def = get_peer_def( buf[0], buf[4] )
   if buf[1] == 'TABLE_DUMP2' or buf[1] == 'TABLE_DUMP':
      if peer_def in DOWN_PEERS: del( DOWN_PEERS[peer_def] )
      update_table_with_dump(buf[0], buf[2], buf[4], buf[6], buf[7])
   elif buf[1] == 'BGP4MP':
      if peer_def in DOWN_PEERS: del( DOWN_PEERS[peer_def] )
      update_type = buf[3]
      if update_type == 'A':
         process_announce( buf[0], buf[2], buf[4], buf[6], buf[7] )
      elif update_type == 'W':
         process_withdraw( buf[0], buf[2], buf[4], buf[6] )
      elif update_type == 'STATE':
         process_state_change( buf[0], buf[2], buf[4], buf[6], buf[7] )
      else:
         print "unsupported update msg type: %s" % ( buf )
# [11, 'BGP4MP', '1314836399', 'A', '198.32.160.22', '6461', '66.199.205.0/24', '6461 209 12042 21873', 'IGP', '198.32.160.22', '0', '0', '6461:5997', 'AG', '21873 192.168.0.1', '\n']
   else:
      print "unsupported bgp msg type: %s" % ( buf )

def process_state_change( rrc, ts, peer_ip, old_state, new_state ):
   peer_def = get_peer_def( rrc, peer_ip )
   if new_state == 6:
      if peer_def in DOWN_PEERS:
         del( DOWN_PEERS[peer_def] )
   else:
      if not peer_def in DOWN_PEERS:
         DOWN_PEERS[peer_def] = ts  ## record first time it went down, may be useful for holdtimer etc.
   #print "down peers now: %s " % ( DOWN_PEERS )

def get_peer_def( rrc, peer_ip ):
   return '-'.join([ str(rrc), peer_ip ])

def get_origin_as( aspath_str ):
   o = aspath_str.split(' ')
   return o[-1]

def process_announce( rrc, ts, peer_ip, pfx, aspath_str):
   ACTIVE_PEERS.update( rrc, peer_ip )
   peer_def = get_peer_def( rrc, peer_ip )
   if pfx in PFX:
      PFX[pfx][peer_def] = get_origin_as( aspath_str )
   else:
      PFX[pfx] = { peer_def: get_origin_as( aspath_str ) }
      #print "new pfx! %s" % ( pfx )

def process_withdraw( rrc, ts, peer_ip, pfx):
   ACTIVE_PEERS.update( rrc, peer_ip )
   peer_def = get_peer_def( rrc, peer_ip )
   if pfx in PFX:
      if peer_def in PFX[pfx]:
         del( PFX[pfx][peer_def] )
      if len( PFX[pfx] ) == 0:
         del( PFX[pfx] )
   else:
      pass
      #print "pfx--: %s" % ( pfx )
     

def update_table_with_dump( rrc, ts, peer_ip, pfx, aspath_str):
   peer_def = '-'.join([ str(rrc), peer_ip ])
   ACTIVE_PEERS.update( rrc, peer_ip )
   if pfx in PFX:
      PFX[pfx][peer_def] = get_origin_as( aspath_str )
   else:
      PFX[pfx] = {peer_def: get_origin_as( aspath_str ) }
   #if len( PFX ) % 10000 == 0:
   #  print "pfx size now %d" % len(PFX)

#### analyzing PFX table

def dump_filename_from_ts( ts ):
   dt = datetime.fromtimestamp( int(ts), pytz.utc )
   dir = '/'.join([ PYRIS_OUTPUT_DIR, dt.strftime('%Y-%m-%d')])
   if not os.path.exists( dir ):
          os.makedirs( dir )
   return "%s/aggr.%s00-V.txt.bz2" % ( dir, dt.strftime('%Y%m%d.%H%M') )

def cc_filename_from_ts( ts ):
   dt = datetime.fromtimestamp( int(ts), pytz.utc )
   dir = '/'.join([ PYRIS_OUTPUT_DIR, dt.strftime('%Y-%m-%d')])
   if not os.path.exists( dir ):
          os.makedirs( dir )
   return "%s/riscc.%s.txt.bz2" % ( dir, dt.strftime('%Y-%m-%dT%H:%M:%SZ') )

def analyze_table( ts, ccrtree, ccrtree_ts ):
   AGGR_WRITE=1 ###  enable/disable aggregate file writing (ie. per resource files)
   dt = datetime.fromtimestamp( ccrtree_ts, pytz.utc )
   ccrtree_iso = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
## DEBUG
#   for rrc in RIS_FILES:
#      print "DEBUG: rrc%02d : 1st file %s" % ( rrc, RIS_FILES[rrc]['files'][0] )
#      print "DEBUG: rrc%02d : buf %s" % ( rrc, RIS_FILES[rrc]['buf'] )
   print "## table size now: %d" % ( len(PFX) )
   print "## active peers now: %s" % ( ACTIVE_PEERS.active_peers )
   print "## down peers now: %s" % ( DOWN_PEERS )
   outfile = dump_filename_from_ts( ts )
   if AGGR_WRITE: fh = bz2.BZ2File(outfile,'w',compresslevel=9)
   print "writing to %s (at %s)" % ( outfile, datetime.now().isoformat() )
   ### cc_counts holds v4/v6/asn counts
   cc_counts={}
   ### asn_power holds number of peers that see a particular ASN
   asn_power={}
   ### debug
   #if '110.196.16.0/22' in PFX:
   #   print "DEBUG: table %s" % ( PFX['110.196.16.0/22'] )
   #else:
   #   print "DEBUG: not in table '110.196.16.0/22'"
   for pfx in PFX:
      cnt = 0
      ases = {}
      pfx_power=0
      pfx_cc = ccrtree.getCcFromPfx( pfx )
      if not pfx_cc:
         pfx_cc = 'XX'
      for peer_def in PFX[pfx]:
         if not peer_def in DOWN_PEERS:
            asn = PFX[pfx][peer_def]
            pfx_power += 1
            if asn in ases:
               ases[asn] += 1
            else:
               ases[asn]=1
            if asn in asn_power:
               asn_power[asn].add( peer_def )
            else:      
               asn_power[asn]=set([ peer_def ])
      for asn in ases:
         #str = '|'.join(['','aggr',pfx,asn, "@%d" % ( ases[asn] ) ]) 
         ### fh.write('|aggr|%s|%s|@%d\n' % ( pfx, asn, ases[asn] ) )
         asn_cc = ccrtree.getCcFromAsn( asn )
         if not asn_cc:
            asn_cc='XX'
         if AGGR_WRITE: fh.write('|aggr|%s|%s|%s|%s|@%d\n' % ( pfx, asn, pfx_cc, asn_cc, ases[asn] ) )
      if pfx_power >= PYRIS_MIN_POWER: ## this pfx is defined as routed, lookup it's country
         rtype = 'v4'
         if pfx.count(':'): # ipv6
            rtype = 'v6'
         if pfx_cc in cc_counts:
            if rtype in cc_counts[pfx_cc]:
               cc_counts[pfx_cc][rtype] += 1
            else:
               cc_counts[pfx_cc][rtype] = 1
         else:
            cc_counts[pfx_cc] = { rtype: 1 }
   for asn in asn_power:
      if len( asn_power[asn] ) >= PYRIS_MIN_POWER:
         asn_cc = ccrtree.getCcFromAsn( asn )
         if not asn_cc:
            asn_cc = 'XX' ### LOG resources unaccounted for
         if asn_cc in cc_counts:
            if 'asn' in cc_counts[asn_cc]:
               cc_counts[asn_cc]['asn'] += 1
            else:
               cc_counts[asn_cc]['asn'] = 1
         else:
            cc_counts[asn_cc] = { 'asn': 1 }
   if AGGR_WRITE: fh.close()

   cc_file = cc_filename_from_ts( ts )
   cc_file_tmp = cc_file + '.TMP' ## avoids reading empty files
   ccfh = bz2.BZ2File(cc_file_tmp, 'w',compresslevel=9)
   print "writing to %s (at %s)" % ( cc_file, datetime.now().isoformat() )
#(pyris)(pyris)bash-3.2$ bzcat riscc.2007-09-16T17\:30\:00Z.txt.bz2 | head
#AD|21|0|1|3|1|1|2007-09-13T00:00:00Z||
#AE|160|1|6|17|1|8|2007-09-13T00:00:00Z||
   for cc in sorted(cc_counts, key=cc_counts.get):
      v4=0
      v6=0
      asn=0
      if 'v4' in cc_counts[cc]:
         v4=cc_counts[cc]['v4']
      if 'v6' in cc_counts[cc]:
         v6=cc_counts[cc]['v6']
      if 'asn' in cc_counts[cc]:
         asn=cc_counts[cc]['asn']
      (v4_s,v6_s,asn_s) = ccrtree.getCcStats( cc )
      ccfh.write('%s|%d|%d|%d|%d|%d|%d|%s||\n' % ( cc, v4, v6, asn, v4_s, v6_s, asn_s, ccrtree_iso ) )
   ccfh.close()
   os.rename(cc_file_tmp, cc_file)
   #h = hpy()
   #print h.heap()

## maybe rewrite as taking multiple RRCs as arguments?
def purge_rrc_from_table( rem_rrc ):
   try:
      rem_rrc_str = str( rem_rrc )
      ### walking through the PFX table is expensive, so try to avoid this
      if ACTIVE_PEERS.is_rrc_active( rem_rrc ):
         cnt=0
         for pfx in PFX:
            rem_peers = [] 
            for peer_def in PFX[pfx]:
               (rrc, peer_ip) = peer_def.split('-')
               if rrc == rem_rrc_str:
                  rem_peers.append( peer_def )
            for peer_def in rem_peers:
               del( PFX[pfx][peer_def] )
               cnt += 1
         ACTIVE_PEERS.reset_rrc( rem_rrc )
         print "finished purge for rrc%02d, cleaned %d" % ( rem_rrc, cnt)
      else:
         print "purge not needed for %02d" % ( rem_rrc )
   except:
      print "exception in pruge_rrc_from table :( %s " % ( traceback.format_exc() )

def main():
   parser = argparse.ArgumentParser(description="fetch and process RIS RIB+update files")
   parser.add_argument('start', metavar='s', type=int, 
                   help='start timestamp')
   parser.add_argument('end', metavar='d', type=int, 
                   help='end timestamp, NOT inclusive')
   parser.add_argument('interval', metavar='i', type=int, 
                   help='interval')

   args = parser.parse_args()
   start_ts = args.start
   end_ts = args.end
   start_dt = datetime.fromtimestamp( args.start, pytz.utc )
   end_dt = datetime.fromtimestamp( args.end, pytz.utc )
   interval = args.interval

   print '%s (start %s, end %s, every %s), started at %s' % ( sys.argv[0], start_dt, end_dt, interval, datetime.now().isoformat() )

   #/@@ TODO: liberal input args: fix_args( args )

   ccrtree=CcRtree()
   ccrtree_ts = int( ( args.start-3*86400 ) / 86400 ) * 86400 # 3 days earlier then start_t, at begin of day 00:00:00Z
   read_cc_stats_from_inrdb( ccrtree, ccrtree_ts )
   ### check availability of cc-stats (for representative countries in each RIR) NOTE this may not work for too old data
   while not ccrtree.checkIfFilled( ['BR','DE','ZA','US','AU'] ):
      print 'cc_stats not OK, retrying in 30 secs'
      time.sleep(30)
      read_cc_stats_from_inrdb( ccrtree, ccrtree_ts )

   print "cc_stats successfully loaded"

   RRCS={}
   for rrc in PYRIS_RRCS:
      fq = RRCFileQueue(rrc)
      fq.fetch_from_webdir( start_ts, end_ts )
      RRCS[rrc] = fq

   next_analyze_ts = start_ts
   while next_analyze_ts < args.end: 
      unprocessed_rrcs=list(PYRIS_RRCS)
      rrc_majority = math.ceil( ( len(PYRIS_RRCS) + 1.0 ) / 2 )
      majority_ts=None
      print "start processing for: %s upto %s " % ( unprocessed_rrcs, next_analyze_ts )
      while len(unprocessed_rrcs) != 0:
         print "BEGIN Loop: unprocessed: %s , majority_ts: %s" % ( len( unprocessed_rrcs ), majority_ts )
         if majority_ts:
            if majority_ts + PYRIS_DELAY_AFTER_MAJORITY < time.time():
               print "giving up on minority: %s at %s" % ( unprocessed_rrcs, time.time() )
               break ### BREAKING OUT!!
            else:
               print "time till giving up on minority: %s " % ( majority_ts + PYRIS_DELAY_AFTER_MAJORITY - time.time() )
         for rrc in list(unprocessed_rrcs):
            lines_read = RRCS[rrc].read_upto( next_analyze_ts, update_table )
            if RRCS[rrc].last_opened_file_ts and RRCS[rrc].last_opened_file_ts + PYRIS_UPDATE_FILE_LENGTH >= next_analyze_ts:
                  print "rrc%02d: all done upto %s ( last opened file %s / diff %s )" % (rrc, next_analyze_ts, RRCS[rrc].last_opened_file_ts, next_analyze_ts - RRCS[rrc].last_opened_file_ts )
                  unprocessed_rrcs.remove( rrc )
         ### set majority_ts 
         if not majority_ts and len( unprocessed_rrcs ) < rrc_majority:
            majority_ts = time.time()
            print "majority of RRCs reached at %s (rrcs left: %d)" % (majority_ts, len(unprocessed_rrcs) )
         if len( unprocessed_rrcs) > 0:
            print "unprocessed_rrcs remaining: %s" % ( unprocessed_rrcs )
            time.sleep(30)
            for rrc in unprocessed_rrcs:
               if RRCS[rrc].last_opened_file_ts:
                  ## fetch an update, starting from the last opened file
                  RRCS[rrc].fetch_from_webdir( RRCS[rrc].last_opened_file_ts, end_ts, fresh=False )
               else: # no files opened yet, try a fresh fetch (and find a bview)
                  RRCS[rrc].fetch_from_webdir( start_ts, end_ts, fresh=True )
         print "END Loop: unprocessed: %s , majority_ts: %s" % ( len( unprocessed_rrcs ), majority_ts )
      ### at this point a majority of RRCs should be processed or been waited for long enough
      for rrc in unprocessed_rrcs:
            purge_rrc_from_table( rrc )
            print "purged rrc %s" % ( rrc )
      analyze_table( next_analyze_ts, ccrtree, ccrtree_ts )
      next_analyze_ts += interval
      print "end processing for: %s " % ( next_analyze_ts )

if __name__ == "__main__":
   main()
   #cProfile.run('main()', 'fooprof')
