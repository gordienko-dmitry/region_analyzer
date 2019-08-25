from optparse import OptionParser
from aiohttp import web
import logging

import api.settings as settings
from api import handlers


def add_handlers(app):
    """
    Adding handlers for web application
    """
    app.add_routes([web.get('', handlers.get_help_handle),
                    web.post('/imports', handlers.import_handle),
                    web.patch('/imports/{import_id}/citizens/{citizen_id}', handlers.patch_handle),
                    web.get('/imports/{import_id}/citizens', handlers.get_citizens_handle),
                    web.get('/imports/{import_id}/citizens/birthdays', handlers.get_presents_handle),
                    web.get('/imports/{import_id}/towns/stat/percentile/age', handlers.percentile_handle)
                    ])


if __name__ == '__main__':
    op = OptionParser(add_help_option=False)
    op.add_option("-h", "--host", action="store", type=str, default='localhost')
    op.add_option("-r", "--route", action="store", type=str, default='')
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()

    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Starting server at %s" % opts.port)

    # Start web server
    app = web.Application()
    add_handlers(app)
    if opts.route:
        web.run_app(app, path=opts.route)
    else:
        web.run_app(app, host=opts.host, port=opts.port)

