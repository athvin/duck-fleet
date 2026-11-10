"""Reject deletions, replacements and resources outside DuckFleet storage/images."""
import json
import sys

ALLOWED = {
    'aws_s3_bucket.lake', 'aws_s3_bucket_public_access_block.lake',
    'aws_s3_bucket_ownership_controls.lake',
    'aws_s3_bucket_server_side_encryption_configuration.lake',
    'aws_s3_bucket_versioning.lake', 'aws_s3_bucket_policy.lake',
    'aws_s3_bucket_lifecycle_configuration.lake',
    'aws_ecr_repository.images["worker"]', 'aws_ecr_repository.images["dbt"]',
}


def validate(plan):
    if plan.get('errored') or not plan.get('complete', False):
        raise ValueError('Plan is errored or incomplete')
    for resource in plan.get('resource_changes', []):
        change = resource['change']
        if resource['address'] not in ALLOWED or resource.get('mode') != 'managed':
            raise ValueError('Resource outside DuckFleet storage/images: ' + resource['address'])
        if change['actions'] not in (['no-op'], ['create'], ['update']):
            raise ValueError('Destructive action refused: ' + resource['address'])
        if change.get('importing') or resource.get('previous_address'):
            raise ValueError('Import/move requires a separate reviewed migration')
    return True


if __name__ == '__main__':
    validate(json.load(sys.stdin))
    print('Plan contains only additive/in-place DuckFleet storage and image changes')
