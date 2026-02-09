"""Cliente Dropbox simplificado usado para sincronizar planilhas."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from io import BytesIO
from typing import Dict, Iterable, Iterator, Tuple

import requests

@dataclass
class DropboxSettings:
    controle_path: str
    folder_path: str | None = None
    files: Dict[str, str] = field(default_factory=dict)
    condicao_path: str | None = None
    access_token: str | None = None
    refresh_token: str | None = None
    app_key: str | None = None
    app_secret: str | None = None

@dataclass
class TokenCache:
    token: str | None = None
    expires_at: float = 0.0

    def valid(self) -> bool:
        return bool(self.token) and self.expires_at > time.time()


def _renew_token(settings: DropboxSettings, cache: TokenCache) -> str:
    if not (settings.refresh_token and settings.app_key and settings.app_secret):
        raise RuntimeError('Nenhum token Dropbox configurado. Defina refresh token ou access token direto.')

    response = requests.post(
        'https://api.dropboxapi.com/oauth2/token',
        data={'grant_type': 'refresh_token', 'refresh_token': settings.refresh_token},
        auth=(settings.app_key, settings.app_secret),
        timeout=30
    )
    if response.status_code != 200:
        raise RuntimeError(f'Falha ao renovar token Dropbox: {response.text}')
    payload = response.json()
    cache.token = payload.get('access_token')
    cache.expires_at = time.time() + int(payload.get('expires_in', 3600)) - 60
    return cache.token or ''


def get_access_token(settings: DropboxSettings, cache: TokenCache) -> str:
    if cache.valid():
        return cache.token or ''
    if settings.refresh_token:
        return _renew_token(settings, cache)
    if settings.access_token:
        cache.token = settings.access_token
        cache.expires_at = time.time() + 3600
        return cache.token
    raise RuntimeError('Nenhum token Dropbox configurado. Defina refresh token ou access token direto.')


def download_file(path: str, token: str) -> BytesIO:
    url = 'https://content.dropboxapi.com/2/files/download'
    headers = {
        'Authorization': f'Bearer {token}',
        'Dropbox-API-Arg': json.dumps({'path': path})
    }
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f'Falha ao baixar {path}: {response.text}')
    return BytesIO(response.content)


def iter_excel_files(settings: DropboxSettings, cache: TokenCache) -> Iterator[Tuple[str, BytesIO]]:
    token = get_access_token(settings, cache)
    if not settings.folder_path or not settings.files:
        return
    for chave, nome in settings.files.items():
        caminho = f"{settings.folder_path.rstrip('/')}/{nome}"
        yield chave, download_file(caminho, token)
