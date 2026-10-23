"""Keep Docker attempts in separate directories, with a current-attempt view.

Only negotiated on the first dispatch. Historical executions keep their existing
layout; no live legacy writer is redirected into a new attempt's directory.
"""
import os
from pathlib import Path
import shutil
import yaml

from app.config import settings

CAPABILITY = 'attempt_artifacts_v1'
DIRECTORIES = ('bundle', 'results', 'progress', 'logs', 'checkpoints')
FILES = ('config.resolved.yaml', 'job_info.json')


def prepare(job_id, attempt):
    job = Path(settings.JOBS_DIR) / job_id
    relative = f'.attempts/{attempt}/jobs/{job_id}'
    target = job / relative
    target.mkdir(parents=True, exist_ok=True)
    # The original input is retained separately from the runner's expanded config.
    original = job / 'config.submitted.yaml'
    if not original.exists():
        shutil.copy2(job / 'config.resolved.yaml', original)
    shutil.copy2(original, target / 'config.resolved.yaml')
    config = yaml.safe_load((target / 'config.resolved.yaml').read_text())
    context = ((config.get('tracking') or {}).get('tags') or {}).get('energaize_training')
    if isinstance(context, dict):
        context['attempt_number'] = attempt
        (target / 'config.resolved.yaml').write_text(yaml.safe_dump(config, sort_keys=False))
    info = job / 'job_info.json'
    if info.is_file() and info.resolve() != (target / 'job_info.json').resolve():
        shutil.copy2(info, target / 'job_info.json')
    for name in DIRECTORIES:
        (target / name).mkdir(exist_ok=True)
    (target / '.worker').mkdir(exist_ok=True)
    (job / '.worker').mkdir(exist_ok=True)
    for name in (*DIRECTORIES, *FILES, '.worker/result-storage.json'):
        current = job / name
        # First-dispatch log/config scaffolding is preserved, never erased.
        if current.exists() and not current.is_symlink():
            previous = job / '.attempts' / 'before-isolation' / name
            previous.parent.mkdir(parents=True, exist_ok=True)
            if previous.exists():
                raise RuntimeError('Unexpected existing artifact during attempt isolation')
            current.rename(previous)
        temporary = current.with_name('.' + current.name + '.attempt-link')
        temporary.unlink(missing_ok=True)
        temporary.symlink_to(os.path.relpath(target / name, current.parent))
        os.replace(temporary, current)
    return {'base': f'jobs/{job_id}/.attempts/{attempt}', 'job_dir': f'jobs/{job_id}/{relative}'}
