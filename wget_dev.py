### wget_dev.py
### adapted from wget.py
### C Narlesky, MBK Engineers
### 08 Apr 2014

import urllib2, cookielib

from wget import *


def download(url, bar=bar_adaptive, setflname = False, flname='inputName.txt'):
    """High level function, which downloads URL into tmp file in current
    directory and then renames it to filename autodetected from either URL
    or HTTP headers.

    :param bar: function to track download progress (visualize etc.)
    :return:    filename where URL is downloaded to
    """

    # 07 April 2014 - CN added setflname, flname to method args to allow 
    # user-specified filename for downloaded content

    if setflname:                        #
        filename = flname                #
    else:                                #

        filename = filename_from_url(url) or "."
    # get filename for temp file in current directory
    (fd, tmpfile) = tempfile.mkstemp(".tmp", prefix=filename+".", dir=".")
    os.close(fd)
    os.unlink(tmpfile)

    # set progress monitoring callback
    def callback_charged(blocks, block_size, total_size):
        # 'closure' to set bar drawing function in callback
        callback_progress(blocks, block_size, total_size, bar_function=bar)
    if bar:
        callback = callback_charged
    else:
        callback = None






    (tmpfile, headers) = urllib.urlretrieve(url, tmpfile, callback)
    filenamealt = filename_from_headers(headers)
    if filenamealt:
        filename = filenamealt
    # add numeric ' (x)' suffix if filename already exists
    if os.path.exists(filename):
        filename = filename #filename_fix_existing(filename)
    shutil.move(tmpfile, filename)

    #print headers
    return filename

# ####################

#     cj = cookielib.CookieJar()
#     opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
#     login_data = urllib.urlencode({'username' : 'cnrfc', 'j_password' : 'cah2o!'})
#     opener.open(url, login_data)
#     resp = opener.open(url)
#     print resp.read()