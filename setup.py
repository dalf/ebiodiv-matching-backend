import codecs
import os
import re

from setuptools import find_namespace_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file,
        re.M,
    )
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")


long_description = read('README.md')
requirements = list(map(str.strip, open('requirements.txt').readlines()))

setup(
    name="ebiodiv-backend-proxy",
    version="1.0.0",

    url='https://github.com/bitem-heg-geneve/ebiodiv-matching-backend',
    author='BITEM HEG Genève',
    author_email='alexandre.flament@hesge.ch',

    packages=find_namespace_packages(include=['ebiodiv']),
    entry_points={
        'console_scripts': [
            'ebiodiv-backend=ebiodiv.__main__:main',
        ],
    },
    zip_safe=False,
    python_requires=">=3.8",
    install_requires=requirements,
)
