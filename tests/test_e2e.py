
from integrations_testing_framework import *
import tap_klaviyo
import os
import pytest

config_path = os.path.abspath('./sample_config.json')


@assert_stdout_matches('tests/catalog.json')
@intercept_requests('tests/requests/discovery.txt', generate=False)
@with_sys_args(['--config', config_path, '--discover'])
def test_schema():
    tap_klaviyo.main()


# @assert_stdout_matches('tests/responses/click.txt')
# @intercept_requests('tests/requests/click.txt', generate=False)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'click', 'stream')])
# def test_click():
#     tap_klaviyo.main()


@assert_stdout_matches('tests/responses/global_exclusions.txt')
@intercept_requests('tests/requests/global_exclusions.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema(
    'tests/catalog.json', 'global_exclusions', 'stream')])
def test_global_exclusions():
    tap_klaviyo.main()


@assert_stdout_matches('tests/responses/lists.txt')
@intercept_requests('tests/requests/webhook.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'lists', 'stream')])
def test_lists():
    tap_klaviyo.main()


@assert_stdout_matches('tests/responses/campaigns.txt')
@intercept_requests('tests/requests/campaigns.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'campaigns', 'stream')])
def test_campaigns():
    tap_klaviyo.main()


# @write_stdout('tests/responses/bounce.txt')
# @intercept_requests('tests/requests/bounce.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'bounce', 'stream')])
# def test_bounce():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/dropped_email.txt')
# @intercept_requests('tests/requests/dropped_email.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema(
#     'tests/catalog.json', 'dropped_email', 'stream')])
# def test_dropped_email():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/mark_as_spam.txt')
# @intercept_requests('tests/requests/mark_as_spam.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema(
#     'tests/catalog.json', 'mark_as_spam', 'stream')])
# def test_mark_as_spam():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/open.txt')
# @intercept_requests('tests/requests/open.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'open', 'stream')])
# def test_open():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/receive.txt')
# @intercept_requests('tests/requests/receive.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'receive', 'stream')])
# def test_receive():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/unsubscribe.txt')
# @intercept_requests('tests/requests/unsubscribe.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'unsubscribe', 'stream')])
# def test_unsubscribe():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/list_members.txt')
# @intercept_requests('tests/requests/list_members.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema(
#     'tests/catalog.json', 'list_members', 'stream')])
# def test_list_members():
#     tap_klaviyo.main()


# @write_stdout('tests/responses/profiles.txt')
# @intercept_requests('tests/requests/profiles.txt', generate=True)
# @with_sys_args(['--config', config_path, '--catalog', utils.select_schema(
#     'tests/catalog.json', 'profiles', 'stream')])
# def test_profiles():
#     tap_klaviyo.main()
