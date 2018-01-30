try:
    from .version import __version__, version_info
except ImportError:
    version_info = (0, 0, 0, 0, 'a')
    __version__ = '{}.{}.{}.{}+{}'.format(*version_info)

package_info = 'Carbon proxy via HTTP'

authors = (
    ('Dmitry Orlov', 'me@mosquito.su'),
)

authors_email = ", ".join(
    '{}'.format(email) for _, email in authors
)

__license__ = 'Apache 2',
__author__ = ", ".join(
    '{} <{}>'.format(name, email) for name, email in authors
)

# It's same persons right now
__maintainer__ = __author__

__all__ = (
    '__author__', '__author__', '__license__',
    '__maintainer__', '__version__',
    'version_info',
)
