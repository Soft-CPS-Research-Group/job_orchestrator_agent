"""Generic execution client. URLs/tokens use EXECUTION_URL / EXECUTION_TOKEN."""
import argparse
import json
import os
from pathlib import Path

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['submit', 'inputs', 'status', 'result', 'artifacts', 'cancel'])
    parser.add_argument('value', help='JSON request file for submit/inputs, job ID otherwise')
    args = parser.parse_args()
    base = os.environ['EXECUTION_URL'].rstrip('/') + '/api/execution/v1'
    headers = {'Authorization': 'Bearer ' + os.environ['EXECUTION_TOKEN']}
    with httpx.Client(timeout=300, headers=headers) as client:
        if args.operation in {'submit', 'inputs'}:
            path = '/jobs' if args.operation == 'submit' else '/inputs'
            response = client.post(base + path, json=json.loads(Path(args.value).read_text()))
        elif args.operation == 'cancel':
            response = client.post(base + '/ops/jobs/' + args.value + '/cancel', json={'reason': 'client_cancel'})
        else:
            response = client.get(base + '/' + args.operation + '/' + args.value)
        if response.is_error:
            parser.exit(1, f'Execution API returned {response.status_code}: {response.text}\n')
        print(json.dumps(response.json(), indent=2))


if __name__ == '__main__':
    main()
