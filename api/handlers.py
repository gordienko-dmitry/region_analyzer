from aiohttp import web

import api.help_text as help
import api.logic as logic


async def get_help_handle(_):
    """
    Handler for getting help message
    """
    return web.Response(text=help.HELP)


async def import_handle(request):
    """
    Handler for import citizens
    """
    return await logic.start_process(request, 'import')


async def patch_handle(request):
    """
    Handler for updating citizen
    """
    return await logic.start_process(request, 'update')


async def get_citizens_handle(request):
    """
    Handler for gettind citizens
    """
    return await logic.start_process(request, 'get')


async def get_presents_handle(request):
    """
    Handler for presents
    """
    return await logic.start_process(request, 'get_presents')


async def percentile_handle(request):
    """
    Handler for percentile
    """
    return await logic.start_process(request, 'get_percentile')

