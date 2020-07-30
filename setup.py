from setuptools import setup

VERSION = '0.0.1'

setup(
    name='kafka-offset-migrate',
    packages=['kafka_offset_migrate'],
    version=VERSION,
    description='Dump offsets from one kafka and set it to another',
    url='https://github.com/Cobliteam/kafka-offset-migrate',
    download_url='https://github.com/Cobliteam/kafka-offset-migrate/archive/{}.tar.gz'.format(VERSION),
    author='Evandro Sanches',
    author_email='evandro@cobli.co',
    license='MIT',
    install_requires=[
        'kafka-python==1.4.6',
        'kazoo==2.6.1'
    ],
    entry_points={
        'console_scripts': ['kafka-offset-migrate=kafka_offset_migrate.main:main']
    },
keywords='kafkas offset migrate')
