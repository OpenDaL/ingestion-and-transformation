# -*- coding: utf-8 -*-
from . import _loadcfg

__all__ = [
    'harvesters', 'structurers', 'translators', 'analyze', 'settings',
    'exceptions', 'post_processors'
]

sources = {
    s.pop('id'): s for s in _loadcfg.sources()
}
