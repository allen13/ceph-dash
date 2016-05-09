from flask import jsonify
from app.base import ApiResource

class HealthResource(ApiResource):
    endpoint = 'health'
    url_prefix = '/health'
    url_rules = {
        'index': {
            'rule': '/',
        }
    }

    def get(self):
        return jsonify({'health': 'ok'})
