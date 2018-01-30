import os

from pip.req import parse_requirements
from setuptools import setup, find_packages


from importlib.machinery import SourceFileLoader

module_name = 'carbon_proxy'
executable_name = module_name.replace('_', '-')

module = SourceFileLoader(
    module_name,
    os.path.join(module_name, '__init__.py')
).load_module()


def load_requirements(fname):
    return [str(r.req) for r in parse_requirements(fname, session='')]


setup(
    name=module_name.replace('_', '-'),
    version=module.__version__,
    author=module.__author__,
    author_email=module.authors_email,
    license=module.__license__,
    description=module.package_info,
    long_description=open("README.rst").read(),
    platforms="all",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Natural Language :: Russian',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Internet',
    ],
    packages=find_packages(exclude=['tests']),
    install_requires=load_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            '{0} = {1}.client:main'.format(
                executable_name,
                module_name,
            ),
            '{0}-server = {1}.server:main'.format(
                executable_name,
                module_name,
            ),
        ]
    },
    extras_require={
        'develop': load_requirements('requirements.dev.txt'),
    },
)
