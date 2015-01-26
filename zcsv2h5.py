#!/usr/bin/python2.7
# encoding: utf-8
'''
zcsv2h5 -- Convert zipped CSV files into HFS5 storage

zcsv2h5 is a file for bulk conversion of zip-archived CSV files into single HFS5 storage

It defines classes_and_methods

@author:     Wojciech Migda

@copyright:  2015 Wojciech Migda. All rights reserved.

@license:    Apache License 2.0

@contact:    user_email
@deffield    updated: Updated
'''

import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2015-01-25'
__updated__ = '2015-01-25'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

def h5file_from(path):
    import h5py
    
    h5file = h5py.File(path.encode(),'a')
    return h5file

def h5dset_from_array(h5file, array, path):
    import h5py
    dset = None
    
    dset = h5file.require_dataset(path.encode(), array.shape
                                  ,chunks=True
                                  ,compression="gzip"
                                  ,compression_opts=7
                                  ,data=array
                                  ,dtype='d'
                                  )
    return dset

################################################################################

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by Wojciech Migda on %s.
  Copyright 2015 Wojciech Migda. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
#         parser.add_argument("-r", "--recursive", dest="recurse", action="store_true", help="recurse into subfolders [default: %(default)s]")
#         parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
#         parser.add_argument("-i", "--include", dest="include", help="only include paths matching this regex pattern. Note: exclude is given preference over include. [default: %(default)s]", metavar="RE" )
#         parser.add_argument("-e", "--exclude", dest="exclude", help="exclude paths matching this regex pattern. [default: %(default)s]", metavar="RE" )
#         parser.add_argument("-o", "--out_file", dest="outfile", help="Name of the output H5 storage file. [default: %(default)s]", metavar="OUTFILE" )
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument(dest="outfile", help="Name of the output H5 storage file. [default: %(default)s]", metavar="outfile", nargs=1)
        parser.add_argument(dest="archives", help="ZIP archive file with CSV file(s) [default: %(default)s]", metavar="archive", nargs='+')

        # Process arguments
        args = parser.parse_args()

        archives = args.archives
        outfile = args.outfile
        
        h5file = h5file_from(outfile[0])
        
        csv_files = []
        for ar in archives:
            import stat
            from zipfile import ZipFile

            if DEBUG:
                print("Opening archive " + ar)
            zf = ZipFile(open(ar, 'r'))
            if DEBUG:
                print("Done.\n")
            
            csv_files = map(lambda item: item.filename,
                            filter(lambda item: stat.S_ISREG(item.external_attr >> 16) and item.filename.endswith(".csv"),
                                   zf.infolist()))
            pass
        total_csv = len(csv_files[:])
        for (i,csv) in enumerate(csv_files[:]):
            from StringIO import StringIO
            from numpy import genfromtxt,loadtxt
            stream = StringIO(zf.read(csv))
#            array = loadtxt(stream
            array = genfromtxt(stream
                            ,delimiter=','
                            ,skiprows=1
                            ,dtype='f8')
            h5dset_from_array(h5file, array, csv)
            print("{:02.1f}% {:s}".format(100.0 * (i + 1) / total_csv, csv))
            pass
        h5file.close()

#         verbose = args.verbose
#         recurse = args.recurse
#         inpat = args.include
#         expat = args.exclude

#         if verbose > 0:
#             print("Verbose mode on")
#             if recurse:
#                 print("Recursive mode on")
#             else:
#                 print("Recursive mode off")

#         if inpat and expat and inpat == expat:
#             raise CLIError("include and exclude pattern are equal! Nothing will be processed.")
# 
#         for inpath in paths:
#             ### do something with inpath ###
#             print(inpath)

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
        pass
#         sys.argv.append("-h")
#         sys.argv.append("-v")
#         sys.argv.append("-r")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'zcsv2h5_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
