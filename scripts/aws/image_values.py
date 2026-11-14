"""Reject incomplete platform indexes before writing Helm's digest references."""
import json
import re
from pathlib import Path

REGISTRY = '797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev'


def image_reference(component, digest, index):
    if component not in ('worker', 'dbt') or not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise ValueError('Invalid component or digest')
    platforms = []
    for manifest in index.get('manifests', []):
        platform = manifest.get('platform', {})
        pair = (platform.get('os'), platform.get('architecture'))
        if (pair == ('unknown', 'unknown') and manifest.get('annotations', {}).get(
                'vnd.docker.reference.type') == 'attestation-manifest'):
            continue
        platforms.append(pair)
    if len(platforms) != 2 or any(platforms.count(pair) != 1 for pair in [('linux', 'amd64'), ('linux', 'arm64')]):
        raise ValueError('Expected exactly one linux/amd64 and linux/arm64 image')
    return f'{REGISTRY}/{component}@{digest}'


def main():
    root = Path('artifacts/aws')
    values = {}
    for component in ('worker', 'dbt'):
        digest = (root/f'{component}-digest.txt').read_text().strip()
        index = json.loads((root/f'{component}-index.json').read_text())
        values[component+'Image'] = image_reference(component, digest, index)
    (root/'values.json').write_text(json.dumps(values, indent=2)+'\n')
    print('Verified both architectures; immutable Helm values written')


if __name__ == '__main__':
    main()
