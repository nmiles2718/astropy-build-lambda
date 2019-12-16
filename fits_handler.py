import logging
from astropy.io import fits


logging.basicConfig(format='%(levelname)-4s '
                           '[%(module)s.%(funcName)s:%(lineno)d]'
                           ' %(message)s',
                    )
LOG = logging.getLogger('fits_handler')
LOG.setLevel(logging.INFO)

class FitsHandler(object):
    def __init__(self, fname=None):

        self._fname = fname
        self._prhdr = None

        # Container for info from both chips
        self._chip1= {
            'sci': None,
            'err': None,
            'dq': None,
        }

        self._chip2= {
            'sci': None,
            'err': None,
            'dq': None,
        }
        # self._chips = [self._chip2, self._chip1]

    @property
    def fname(self):
        """Filename of the FITS image"""
        return self._fname

    @fname.setter
    def fname(self, value):
        self._fname = value

    @property
    def prhdr(self):
        """Primary Header of the FITS image"""
        return self._prhdr

    @prhdr.setter
    def prhdr(self, value):
        self._prhdr = value

    @property
    def chip1(self):
        """Container for storing data for both chips

        A dict with the following attirbutes:

            * sci
            * err
            * dq


        """
        return self._chip1

    @chip1.setter
    def chip1(self, value):
        self._chip1 = value

    @property
    def chip2(self):
        """Container for storing data for both chips

        A dict with the following attirbutes:

            * sci
            * err
            * dq


        """
        return self._chip2

    @chip2.setter
    def chip2(self, value):
        self._chip2 = value

    def get_data(self, ext='dq'):
        """ Grab the data from the specified extension of the FITS file

        Parameters
        ----------
        ext : str
            The name of the extension (`sci`, `err`, `dq`).

        Returns
        -------

        """
        ext1 = (ext, 1)  # Chip 2
        ext2 = (ext, 2)  # Chip 1
        self._prhdr = fits.getheader(self.fname, 0)
        i = 2
        for ext_tuple, chip in zip([ext1, ext2], [self.chip2, self.chip1]):
            with fits.open(self.fname) as hdu:
                try:
                    ext_idx = hdu.index_of(ext_tuple)
                    ext_data = hdu[ext_idx].data
                except KeyError:
                    LOG.info(
                        '{1} is missing for {0}'.format(self._fname, ext1))
                else:
                    chip[ext] = ext_data
                    chip[f"{ext}{i}_hdr"] = hdu[ext_idx].header
                finally:
                    i -=1
