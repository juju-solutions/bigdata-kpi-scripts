#!/usr/bin/env python2

from collections import defaultdict
import glob
import gzip
import re
import os
import logging

from datetime import datetime
from kpi_common import get_push_gateway
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


logs = [
    glob.glob('/var/tmp/logs/api/1/api.jujucharms.com.log-201*'),
    glob.glob('/var/tmp/logs/api/2/api.jujucharms.com.log-201*'),
]

clouds = ['maas', 'ec2', 'azure', 'gce', 'lxd', 'openstack', 'manual']

uuid_re = 'environment_uuid=[\w]{8}-[\w]{4}-[\w]{4}-[\w]{4}-[\w]{12}'
uuid_re_compiled = re.compile(uuid_re)
cloud_re = 'provider=[^,\"]*'
region_re = 'cloud_region=[^,\"]*'
version_re = 'controller_version=[^,\"]*'
app_re = 'meta/any\?id=[\w][^&]*'
app_re_compiled = re.compile(app_re)


def find_uuid(l):
    m = uuid_re_compiled.search(l)
    if m:
        uuid = m.group(0)
        return uuid.split('=')[1]
    else:
        return None


def find_app(l):
    m = app_re_compiled.search(l)
    if m:
        uuid = m.group(0)
        return uuid.split('=')[1]
    else:
        return None


def find_metadata(l, date):
    app = re.search(app_re, l)

    found = None
    if app:
        c = re.search(cloud_re, l)
        cr = re.search(region_re, l)
        v = re.search(version_re, l)
        cloud = 'pre-2'
        region = 'pre-2'
        version = 'pre-2'

        if c:
            _, cloud = c.group().split('=')

        if cr:
            _, region = cr.group().split('=')

        if v:
            _, version = v.group().split('=')

        _, found = app.group().split('=')

        return {
            "app": found,
            "cloud": cloud,
            "region": region,
            "version": version,
            "start": date,
            "end": date,
        }

    return found


def register_active_data(registry, data):
    if len(data) <=0:
        return
    active_gauge = Gauge(
        'live_big_deployments_{}_active'.format(data[0]['app_name']),
        'Active deployment in all clouds of {}'.format(data[0]['app_name']),
        ['active'],
        registry=registry,
    )
    active_deps_num = len(list(filter(lambda deployment: deployment['active'] == True, data)))
    active_gauge.labels('True').set(active_deps_num)

    for cloud in clouds:
        active_in_cloud = Gauge(
            'live_big_deployments_{}_active_{}'.format(data[0]['app_name'], cloud),
            'Active deployments in {} of {}'.format(cloud, data[0]['app_name']),
            ['active'],
            registry=registry,
        )
        active_cloud_deps_num = len(list(filter(lambda deployment:
                                          deployment['active'] == True and
                                          deployment['cloud'] == cloud, data)))
        active_in_cloud.labels('True').set(active_cloud_deps_num)


def register_period(registry, data, label):
    if len(data) <=0:
        return
    all_deployments_gauge = Gauge(
        'live_big_deployments_{}_{}'.format(data[0]['app_name'], label),
        'Deployment in all clouds of {}'.format(data[0]['app_name']),
        registry=registry,
    )
    all_deployments_gauge.set(len(data))

    all_long_deployments_gauge = Gauge(
        'live_big_deployments_longlasting_{}_{}'.format(data[0]['app_name'], label),
        'Deployment in all clouds with lifespan greater than 2 weeks of {}'.format(data[0]['app_name']),
        registry=registry,
    )
    long_deps_num = len(list(filter(lambda deployment: deployment['days'] > 14, data)))
    all_long_deployments_gauge.set(long_deps_num)

    for cloud in clouds:
        deployments_in_cloud = Gauge(
            'live_big_deployments_{}_{}_{}'.format(data[0]['app_name'], cloud, label),
            'Deployments in {} for {}'.format(cloud, label),
            registry=registry,
        )
        cloud_deps_num = len(list(filter(lambda deployment:
                                          deployment['cloud'] == cloud, data)))
        deployments_in_cloud.set(cloud_deps_num)

        long_deployments_in_cloud = Gauge(
            'live_big_deployments_longlasting_{}_{}_{}'.format(data[0]['app_name'], cloud, label),
            'Deployments in {} for {} with lifespan greater than 2 weeks'.format(cloud, label),
            registry=registry,
        )
        long_cloud_deps_num = len(list(filter(lambda deployment: deployment['days'] > 14 and
                                         deployment['cloud'] == cloud, data)))
        long_deployments_in_cloud.set(long_cloud_deps_num)


def gather_lines(app_ids):
    print("Start scanning logs")
    charms_regexps = []
    for app in app_ids:
        app_regex = re.compile(app)
        charms_regexps.append(app_regex)

    meta_lines = []
    app_re_regex = re.compile(app_re)
    for g in logs:
        print("Found logs {0}".format(len(g)))
        logs_proc = 0
        for path in g:
            logs_proc += 1
            logname = os.path.basename(path)
            datestr = logname.\
                replace('api.jujucharms.com.log-', '').\
                replace('.anon', '').\
                replace('.gz', '')
            print("{}/{} : Lines so far {} ".format(logs_proc, len(g), len(meta_lines)))
            print("{}/{} : date {} ".format(logs_proc, len(g), datestr))

            dirname = os.path.dirname(path)
            preprocessed = os.path.join(dirname, 'prep-bigdata-{}.data'.format(datestr))

            if os.path.exists(preprocessed):
                selected_lines = []
                with open(preprocessed) as prep:
                    selected_lines += prep.readlines()
                print("{}/{} : Have preprocessed file.".format(logs_proc, len(g)))
            else:
                with gzip.open(path) as f:
                    lines = f.read().split("\n")

                print("{}/{} : Done unzip. Total lines {}".format(logs_proc, len(g), len(lines)))
                selected_lines = []
                for l in lines:
                    app_found = find_app(l)
                    if app_found and find_uuid(l):
                        for reg in charms_regexps:
                            if reg.search(app_found):
                                selected_lines.append(l)
                                break
                with open(preprocessed, "wt") as prep:
                    for l in selected_lines:
                        prep.write("{}\n".format(l))

            for l in selected_lines:
                meta_lines.append((l, datestr))

    return meta_lines


def gather_stats(lines, registry, app_name, app_id):
    apps = defaultdict(list)
    app_id_expr = re.compile(app_id)
    for ltup in lines:
        l = ltup[0]
        datestr = ltup[1]
        uuid = find_uuid(l)
        if uuid:
            data = find_metadata(l, datestr)
            if not data or not app_id_expr.search(data['app']):
                continue

            data['app_name'] = app_name
            if uuid not in apps:
                apps[uuid] = data
            else:
                data = apps[uuid]
                if data['start'] > datestr:
                    data['start'] = datestr
                if data['end'] < datestr:
                    data['end'] = datestr

    print("Found UUIDs {} for {}".format(len(apps.keys()), app_name))

    logging.basicConfig(level=logging.DEBUG)

    month_dataset = []
    three_month_dataset = []
    six_month_dataset = []
    for uuid, data in apps.items():
        lastdate = datetime.strptime(datestr, '%Y%m%d')
        start = datetime.strptime(data['start'], '%Y%m%d')
        end = datetime.strptime(data['end'], '%Y%m%d')
        days = (end - start).days + 1
        if (lastdate - end).days > 1:
            data['active'] = False
        else:
            data['active'] = True
        data['days'] = days

        today = datetime.now()
        if (today - start).days <= 180:
            six_month_dataset.append(data)
        if (today - start).days <= 90:
            three_month_dataset.append(data)
        if (today - start).days <= 30:
            month_dataset.append(data)
        print("{} {}".format(uuid, data))

    register_active_data(registry, list(apps.values()))
    print("Done with registering active for {}".format(app_name))
    register_period(registry, six_month_dataset, "six_months")
    register_period(registry, three_month_dataset, "three_months")
    register_period(registry, month_dataset, "one_month")
    print("Done with registering periods for {}".format(app_name))


if __name__ == "__main__":
    app_ids = [("spark", ".*cs%3A(xenial|trusty)%2Fspark.*"),
               ("hadoop", ".*cs%3A(xenial|trusty)%2Fhadoop-resourcemanager.*"),
               ("kafka", ".*cs%3A(xenial|trusty)%2Fkafka.*"),
               ("apache_spark", ".*cs%3A(xenial|trusty)%2Fapache-spark.*"),
               ("apache_hadoop", ".*cs%3A(xenial|trusty)%2Fapache-hadoop-resourcemanager.*"),
               ("apache_kafka", ".*cs%3A(xenial|trusty)%2Fapache-kafka.*")]
    regs = []
    for app in app_ids:
        regs.append(app[1])
    lines = gather_lines(regs)
    registry = CollectorRegistry()
    for app in app_ids:
        gather_stats(lines, registry, app[0], app[1])

    pkg = 'bigdata-kpi-scripts'
    name = 'bigdata-live-stats'
    gateway = get_push_gateway(pkg, name)
    push_to_gateway(gateway, job=name, registry=registry)
