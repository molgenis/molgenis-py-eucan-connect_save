# """
# This is an example file that can serve as the starting point for a Python
# console script. To run this script uncomment the lines in the
# ``[options.entry_points]`` section in ``setup.cfg``
#
# Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
# which will install the command ``example`` inside your current environment.
#
# Note:
#     This file can be safely removed if not needed!
#
# References:
#     - https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html
#     - https://pip.pypa.io/en/stable/reference/pip_install
# """
#
# import argparse
# import logging
# import sys
#
# from molgenis.eucan_connect import __version__
#
# _logger = logging.getLogger(__name__)
#
#
# def parse_args(args):
#     """Parse command line parameters
#
#     Args:
#       args (List[str]): command line parameters as list of strings
#           (for example  ``["--help"]``).
#
#     Returns:
#       :obj:`argparse.Namespace`: command line parameters namespace
#     """
#     parser = argparse.ArgumentParser(description="Just an example")
#     parser.add_argument(
#         "--version",
#         action="version",
#         version="molgenis-py-eucan_connect {ver}".format(ver=__version__),
#     )
#     parser.add_argument(
#         "-v",
#         "--verbose",
#         dest="loglevel",
#         help="set loglevel to INFO",
#         action="store_const",
#         const=logging.INFO,
#     )
#     parser.add_argument(
#         "-vv",
#         "--very-verbose",
#         dest="loglevel",
#         help="set loglevel to DEBUG",
#         action="store_const",
#         const=logging.DEBUG,
#     )
#     return parser.parse_args(args)
#
#
# def setup_logging(loglevel):
#     """Setup basic logging
#
#     Args:
#       loglevel (int): minimum loglevel for emitting messages
#     """
#     logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
#     logging.basicConfig(
#         level=loglevel,
#         stream=sys.stdout,
#         format=logformat,
#         datefmt="%Y-%m-%d %H:%M:%S"
#     )
#
#
# def main(args):
#     """Wrapper allowing :func:`fib` to be called with string arguments
#     in a CLI fashion
#
#     Args:
#       args (List[str]): command line parameters as list of strings
#           (for example  ``["--verbose", "42"]``).
#     """
#     args = parse_args(args)
#     setup_logging(args.loglevel)
#     print("Hi!")
#
#
# def run():
#     """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`"""
#     main(sys.argv[1:])
#
#
# if __name__ == "__main__":
#     # ^  This is a guard statement that will prevent the following code from
#     #    being executed in the case someone imports this file instead of
#     #    executing it as a script.
#     #    https://docs.python.org/3/library/__main__.html
#
#     # After installing your project with pip, users can also run your Python
#     # modules as scripts via the ``-m`` flag, as defined in PEP 338::
#     #
#     #     python -m molgenis.eucan_connect.skeleton 42
#     #
#     run()
