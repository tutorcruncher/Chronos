from fastapi import Depends
from fastapi_admin.app import app as admin_app
from fastapi_admin.depends import get_resources
from fastapi_admin.template import templates
from starlette.requests import Request


@admin_app.get('/')
async def home(request: Request, resources=Depends(get_resources)):
    return templates.TemplateResponse('dashboard.html', context={'request': request, 'resources': resources})
