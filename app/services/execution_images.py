"""Freeze Docker Hub manifests once, before persisting submission intent."""
import re

import httpx
from fastapi import HTTPException

from app.config import settings


def resolve(image):
    repository, tag = image.rsplit(':', 1)
    with httpx.Client(timeout=15) as client:
        try:
            token = client.get('https://auth.docker.io/token', params={
                'service': 'registry.docker.io', 'scope': f'repository:{repository}:pull'})
            token.raise_for_status()
            response = client.get(f'https://registry-1.docker.io/v2/{repository}/manifests/{tag}',
                headers={'Authorization': 'Bearer ' + token.json()['token'], 'Accept': ', '.join([
                    'application/vnd.oci.image.index.v1+json',
                    'application/vnd.docker.distribution.manifest.list.v2+json',
                    'application/vnd.oci.image.manifest.v1+json',
                    'application/vnd.docker.distribution.manifest.v2+json'])})
            if response.status_code == 404:
                return None
            response.raise_for_status()
            digest = response.headers.get('docker-content-digest', '')
            if not re.fullmatch(r'sha256:[a-f0-9]{64}', digest):
                raise ValueError('Invalid registry digest')
            return repository + '@' + digest
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            raise HTTPException(503, 'Cannot freeze execution image; retry later') from exc


def freeze(request):
    from app.services import job_service
    preferred = job_service._preferred_host(request.target_host)
    hosts = [preferred] if preferred else settings.AVAILABLE_HOSTS
    images, cache = {}, {}
    tag = job_service._normalize_image_tag(request.image_tag)
    for host in hosts:
        # These destinations keep their existing legacy/SIF contracts, out of
        # this release's CPU/Union acceptance scope.
        if host == 'deucalion' or job_service._is_jetson_worker(host):
            continue
        image, _ = job_service._resolve_job_image_for_worker(host, tag)
        if image not in cache:
            cache[image] = resolve(image)
        if cache[image]:
            images[host] = cache[image]
    if not images:
        raise HTTPException(400, 'No immutable image is available for the selected destination')
    return images
