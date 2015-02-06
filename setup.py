from setuptools import setup

setup(
    name='jqueue',
    packages=['jqueue'],
    entry_points = {
        'console_scripts':
        ['jqueue-server = jqueue.cli:runner']
    },
    author='Adam Marchetti',
    version='0.1.2',
    description='A queue for distributed job processing',
    author_email='adamnew123456@gmail.com',
    keywords=['networking'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'License :: OSI Approved :: BSD License',
        'Development Status :: 3 - Alpha',
        'Topic :: System :: Networking',
        'Topic :: Utilities',
    ],
    long_description="Provides a networked job queue for processing jobs via a network.")
