# Releases

Este ficheiro e o changelog operacional do `job_orchestrator_agent`. A partir de agora, cada bump de versao deve explicar o que mudou, impacto para utilizadores, impacto em storage/API, validacao e notas de migracao.

Versao inglesa: [../releases.md](../releases.md).

Responsavel default por releases: [@calofonseca](https://github.com/calofonseca).

## Politica de Versao

| Tipo | Quando usar | Exemplo |
| --- | --- | --- |
| Patch | Fixes compativeis e pequenas mudancas aditivas. | `1.0.0 -> 1.0.1` |
| Minor | Nova capacidade ou novo grupo de endpoints compativel. | `1.0.x -> 1.1.0` |
| Major | Breaking change em API, storage ou contrato com workers. | `1.x -> 2.0.0` |

## Checklist por Release

1. Atualizar `pyproject.toml`.
2. Atualizar `app/version.py`.
3. Atualizar este ficheiro com notas de release.
4. Atualizar `README.md`, Postman e docs de endpoints quando mudarem contratos publicos.
5. Correr validacao:
   - `.venv/bin/pytest`
   - `python3 -m compileall app`
   - `docker build -t job_orchestrator_agent:test .`
   - `docker run --rm job_orchestrator_agent:test python -c "from app.main import app; print(app.version)"`
6. Validar compose de infra quando mudar deployment:
   - `docker compose -f ../opeva_infra_services/job_orchestrator_agent/docker-compose.yaml config`
7. Criar commit e tag.
8. Fazer push de `main` e da tag `vX.Y.Z`.
9. Confirmar que o GitHub Actions publicou as tags Docker:
   - `calof/job_orchestrator_agent:<commit-sha>`
   - `calof/job_orchestrator_agent:latest` a partir de `main`
   - `calof/job_orchestrator_agent:vX.Y.Z` a partir de tags de release

## Template

```markdown
## vX.Y.Z - YYYY-MM-DD

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary
- ...

### Added
- ...

### Changed
- ...

### Fixed
- ...

### API/Storage Impact
- ...

### Compatibility
- ...

### Validation
- `...`: pass

### Migration Notes
- ...
```

## v1.0.0 - 2026-05-16

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary

Primeira release estavel do OPEVA Job Orchestrator como servico standalone. O servico passa a ser dono do lifecycle de jobs, coordenacao com worker agents, experiment configs, datasets de jobs, artefactos de simulation-data e controlos operacionais.

### Added

- Entrypoint FastAPI `uvicorn app.main:app` na porta `8011`.
- Endpoints de jobs para lancamento, listagem, fila, estado, progresso, resultados, logs, resolved config, hosts e versoes de imagens.
- Endpoints de worker agent para polling de jobs, atualizacao de estado e heartbeats.
- Captura da versao runtime dos workers via heartbeat e job-status, exposta em `/hosts`.
- Endpoints ops para requeue, stop, fail, cancel e cleanups.
- Endpoints de experiment configs em `/experiment-config*`.
- Endpoints de datasets em `/dataset*` e `/datasets`, incluindo geracao CityLearn via Mongo e upload/download ZIP.
- Endpoints de simulation-data para leitura de artefactos de resultados de jobs.
- Dockerfile, workflow CI e defaults de publish Docker Hub para `calof/job_orchestrator_agent`.
- Colecao Postman com jobs, datasets, configs, agent, ops e simulation-data.

### Changed

- Contratos relacionados com jobs saem do `opeva_backend_api_training`.
- Base URL publica de jobs/datasets/configs muda da porta `8000` do backend para a porta `8011` do orchestrator.
- URL interna dos workers passa a ser `http://job_orchestrator_agent:8011`.

### Fixed

- Criacao de diretorios de startup passa para o lifespan do FastAPI, evitando writes em `/opt/opeva_shared_data` no import do modulo.
- Package discovery fica limitado a `app*`, evitando que docs/Postman interfiram com builds setuptools.
- Ranges de dependencias Docker/CI ficam constrangidos para installs estaveis da geracao de datasets.

### API/Storage Impact

- O orchestrator passa a ser dono de `/opt/opeva_shared_data/configs`, `/datasets`, `/jobs`, `/queue` e `job_track.json`.
- Paths e payloads existentes sao preservados; so muda o servico/porta.
- Nao fica proxy temporario no `opeva_backend_api_training`.

### Compatibility

- Compativel com o codigo atual dos worker agents quando `OPEVA_SERVER` aponta para `http://job_orchestrator_agent:8011`.
- Clientes devem atualizar pedidos de jobs, configs, datasets e simulation-data para a porta `8011`.
- Backend API continua disponivel na porta `8000` para Mongo/schema/deploy/real-time/health.

### Validation

- `.venv/bin/pytest`: pass (`109 passed`)
- `python3 -m compileall app`: pass
- `docker build -t job_orchestrator_agent:test .`: pass
- `docker run --rm job_orchestrator_agent:test python -c "from app.main import app; print(app.version)"`: pass
- `docker compose -f ../opeva_infra_services/job_orchestrator_agent/docker-compose.yaml config`: pass

### Migration Notes

- Atualizar frontend/API clients de `http://<host>:8000` para `http://<host>:8011` em jobs, configs, datasets e simulation data.
- Atualizar worker deployments para `OPEVA_SERVER=http://job_orchestrator_agent:8011`.
- Manter `/opt/opeva_shared_data` montado no mesmo path no orchestrator e em todos os workers.
