Prototype code to create periodic snapshots of BGP data combining RIS dump + updates files.

See top of pyris.py for list of variables (starting with PYRIS) that can be configured.
Configuration is done by putting variables in a local.py file, using python syntax.
The least you should do is point PYRIS to a working version of bgpdump:
    PYRIS_CMD_BGPDUMP='/Users/emile/bin/bgpdump'

To run pyris:
    pyris.py start_ts end_ts interval
  
Example to create 5min interval files for the first 8 hrs of 2014-04-01 (UTC):
    pyris.py 1396310400 1396339200 300
