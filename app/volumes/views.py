#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import glob
import os
import subprocess
import ConfigParser
import dpath

from flask import request
from flask import render_template
from flask import abort
from flask import jsonify
from flask import current_app
from flask.views import MethodView

from rados import Rados
from rados import Error as RadosError
import rbd

from app.base import ApiResource


def get_ceph_clusters():
    """
    Grab dictionary of ceph clusters from the config directory specified in
    config.json
    """

    config_path = current_app.config['USER_CONFIG']['config_path']
    ceph_clusters = dict()
    for config_file in glob.glob(config_path + '*.conf'):
        cluster_name = os.path.basename(os.path.splitext(config_file)[0])
        ceph_clusters[cluster_name] = dict()
        ceph_clusters[cluster_name]['conffile'] = config_path + cluster_name + '.conf'
        ceph_clusters[cluster_name]['conf'] = dict(keyring = config_path + cluster_name + '.keyring')

    return ceph_clusters


def parse_ceph_config(conffile):
    """
    Parses a ceph config file and returns a dictionary
    """

    ceph_config = dict()
    config = ConfigParser.SafeConfigParser()
    config.read(conffile)

    for section in config.sections():
        ceph_config[section] = dict()
        for option in config.items(section):
            ceph_config[section][option[0]] = option[1]

    for default in config.defaults():
        ceph_config[default[0]] = default[1]

    return ceph_config

def get_ceph_config_monitors(ceph_config):
    """
    Takes a ceph config as a python dictionary and returns the list of monitors
    """

    monitors = dpath.util.values(ceph_config, 'mon.*/mon_addr')

    return monitors


def get_rbd_images(ceph_pool = 'rbd'):
    """
    Grab a dictionary of rbd images in a pool across all clusters
    """

    all_images = dict()
    for cluster_name, cluster_config in get_ceph_clusters().iteritems():
        all_images[cluster_name] = []
        ceph_config = parse_ceph_config(cluster_config['conffile'])
        ceph_monitors = get_ceph_config_monitors(ceph_config)
        with Rados(**cluster_config) as cluster:
            with cluster.open_ioctx(ceph_pool) as ioctx:
                rbd_inst = rbd.RBD()
                for rbd_image_name in rbd_inst.list(ioctx):
                    with rbd.Image(ioctx, rbd_image_name) as rbd_image:
                        rbd_size = (rbd_image.size() / 1000000000)
                        rbd_data = {
                            'name': rbd_image_name,
                            'size': rbd_size,
                            'monitors': ceph_monitors
                        }
                        all_images[cluster_name].append(rbd_data)

    return all_images


def get_openshift_pvs():
    """
    Grab a list of openshift projects
    """

    openshift_pvs = json.loads(subprocess.check_output(["oc", "get", "pv", "-o", "json"]))['items']

    return openshift_pvs


def get_ceph_openshift_volumes(ceph_pool = 'rbd'):
    """
    Matches ceph volumes with openshift persistent volumes and returns a dict
    """

    ceph_openshift_images = get_rbd_images(ceph_pool)
    openshift_pvs = get_openshift_pvs()

    for cluster_name, rbd_images in ceph_openshift_images.iteritems():
        for rbd_image in rbd_images:
            for openshift_pv in openshift_pvs:
                if ((openshift_pv['spec']['rbd']['image'] == rbd_image['name']) and
                    (set(openshift_pv['spec']['rbd']['monitors']) == set(rbd_image['monitors']))):
                    rbd_image['pv'] = openshift_pv['metadata']['name']
                    if 'claimRef' in openshift_pv['spec']:
                        rbd_image['pvc'] = openshift_pv['spec']['claimRef']['name']
                        rbd_image['project'] = openshift_pv['spec']['claimRef']['namespace']

    return ceph_openshift_images

class CephClusterCommand(dict):
    """
    Issue a ceph command on the given cluster and provide the returned json
    """

    def __init__(self, cluster, **kwargs):
        dict.__init__(self)
        ret, buf, err = cluster.mon_command(json.dumps(kwargs), '', timeout=5)
        if ret != 0:
            self['err'] = err
        else:
            self.update(json.loads(buf))


class VolumesResource(ApiResource):
    """
    Endpoint that shows overall cluster status
    """

    endpoint = 'volumes'
    url_prefix = '/volumes'
    url_rules = {
        'index': {
            'rule': '/',
        }
    }

    def __init__(self):
        MethodView.__init__(self)
        self.config = current_app.config['USER_CONFIG']

    def get(self):
        page_data = dict()
        page_data['rbd_images'] = get_ceph_openshift_volumes()

        if request.mimetype == 'application/json':
            return jsonify(page_data)
        else:
            return render_template('volumes.html', data=page_data, config=get_ceph_clusters())
