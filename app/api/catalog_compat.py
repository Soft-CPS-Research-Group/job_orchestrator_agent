"""Temporary transport adapter. Execution never calls this dependency."""
import httpx
from starlette.responses import Response, JSONResponse

from app.config import settings


async def forward_catalog(request, call_next):
    path = request.url.path
    is_catalog = path in {'/datasets', '/dataset', '/experiment-configs', '/datasets/generate'} or path.startswith(('/dataset/', '/experiment-config/'))
    if not settings.TRAINING_CATALOG_URL or not is_catalog:
        return await call_next(request)
    headers = {'X-Catalog-Token': settings.TRAINING_CATALOG_TOKEN or '',
               'content-type': request.headers.get('content-type', 'application/json')}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300, connect=5)) as client:
            upstream = await client.request(request.method,
                settings.TRAINING_CATALOG_URL.rstrip('/') + '/internal/catalog' + path,
                params=request.query_params, headers=headers, content=await request.body())
        return Response(upstream.content, status_code=upstream.status_code,
            headers={key: upstream.headers[key] for key in ('content-type', 'content-disposition') if key in upstream.headers})
    except httpx.HTTPError:
        return JSONResponse({'detail': 'Training catalog is unavailable; execution remains independent'}, status_code=503)
