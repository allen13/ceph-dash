#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import glob
import os

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


def find_host_for_osd(osd, osd_status):
    """ find host for a given osd """

    for obj in osd_status['nodes']:
        if obj['type'] == 'host':
            if osd in obj['children']:
                return obj['name']

    return 'unknown'


def get_unhealthy_osd_details(osd_status):
    """ get all unhealthy osds from osd status """

    unhealthy_osds = list()

    for obj in osd_status['nodes']:
        if obj['type'] == 'osd':
            # if OSD does not exists (DNE in osd tree) skip this entry
            if obj['exists'] == 0:
                continue
            if obj['status'] == 'down' or obj['reweight'] == 0.0:
                # It is possible to have one host in more than one branch in the tree.
                # Add each unhealthy OSD only once in the list
                if obj['status'] == 'down':
                    status = 'down'
                else:
                    status = 'out'
                entry = {
                    'name': obj['name'],
                    'status': status,
                    'host': find_host_for_osd(obj['id'], osd_status)
                }
                if entry not in unhealthy_osds:
                    unhealthy_osds.append(entry)

    return unhealthy_osds

def get_rbd_images(ceph_pool = 'rbd'):
    """
    Grab a list of rbd images in a pool
    """

    rbd_images = []
    for cluster_name, cluster_config in get_ceph_clusters().iteritems():
        with Rados(**cluster_config) as cluster:
            with cluster.open_ioctx(ceph_pool) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_images = [ {'name': rbd_image, 'cluster_name': cluster_name } for rbd_image in rbd_inst.list(ioctx) ]

    return rbd_images


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


class DashboardResource(ApiResource):
    """
    Endpoint that shows overall cluster status
    """

    endpoint = 'dashboard'
    url_prefix = '/'
    url_rules = {
        'index': {
            'rule': '/',
        }
    }

    def __init__(self):
        MethodView.__init__(self)
        self.config = current_app.config['USER_CONFIG']
        self.clusterprop = get_ceph_clusters().itervalues().next()

    def get(self):
        with Rados(**self.clusterprop) as cluster:
            cluster_status = CephClusterCommand(cluster, prefix='status', format='json')
            if 'err' in cluster_status:
                abort(500, cluster_status['err'])

            # check for unhealthy osds and get additional osd infos from cluster
            total_osds = cluster_status['osdmap']['osdmap']['num_osds']
            in_osds = cluster_status['osdmap']['osdmap']['num_up_osds']
            up_osds = cluster_status['osdmap']['osdmap']['num_in_osds']
            cluster_status['rbd_images'] = get_rbd_images()

            if up_osds < total_osds or in_osds < total_osds:
                osd_status = CephClusterCommand(cluster, prefix='osd tree', format='json')
                if 'err' in osd_status:
                    abort(500, osd_status['err'])

                # find unhealthy osds in osd tree
                cluster_status['osdmap']['details'] = get_unhealthy_osd_details(osd_status)

            if request.mimetype == 'application/json':
                return jsonify(cluster_status)
            else:
                return render_template('status.html', data=cluster_status, config=self.config)


class VolumesResource(ApiResource):
    """
    Endpoint that shows overall cluster status
    """

    endpoint = 'volumes'
    url_prefix = '/volumes'
    url_rules = {
        'index': {
            'rule': '/volumes',
        }
    }

    def __init__(self):
        MethodView.__init__(self)
        self.config = current_app.config['USER_CONFIG']
        self.clusterprop = get_ceph_clusters().itervalues().next()

    def get(self):
        with Rados(**self.clusterprop) as cluster:
            cluster_status = CephClusterCommand(cluster, prefix='status', format='json')
            if 'err' in cluster_status:
                abort(500, cluster_status['err'])

            if request.mimetype == 'application/json':
                return jsonify(cluster_status)
            else:
                return render_template('status.html', data=cluster_status, config=self.config)
